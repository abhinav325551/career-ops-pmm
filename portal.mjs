#!/usr/bin/env node

/**
 * portal.mjs — Live PMM Jobs Portal
 *
 * Fetches PMM roles directly from Greenhouse, Ashby, and Lever APIs,
 * tags each job by region (India / ASEAN / Dubai / Global), and serves
 * a browsable web UI with full JD preview and apply links.
 *
 * Usage:
 *   node portal.mjs
 *   node portal.mjs --port 4000
 */

import { createServer } from 'http';
import { readFileSync, existsSync } from 'fs';
import yaml from 'js-yaml';

const args = process.argv.slice(2);
const portFlag = args.indexOf('--port');
const PORT = portFlag !== -1 ? parseInt(args[portFlag + 1]) : 3333;
const PORTALS_PATH = 'portals.yml';
const CACHE_TTL_MS = 30 * 60 * 1000;
const FETCH_TIMEOUT_MS = 12_000;
const CONCURRENCY = 12;

// ── Region maps ────────────────────────────────────────────────────────────────

const INDIA_COMPANIES = new Set([
  'Sarvam AI','Yellow.ai','Uniphore','Observe.AI','Kore.ai','Freshworks',
  'Whatfix','MoEngage','CleverTap','WebEngage','LeadSquared','Chargebee',
  'Postman','BrowserStack','Hasura','Druva','Mindtickle','Sprinklr',
  'Razorpay','Zoho','Google India','Microsoft India','Adobe India',
  'Salesforce India','HubSpot India','Atlassian India','Zendesk India',
  'Twilio India','Maxim AI',
]);

const ASEAN_COMPANIES = new Set([
  'GoTo Group','Gojek','Ninja Van','Xendit','Aspire','Grab',
  'Carousell Group','PropertyGuru','Traveloka','Razer','StashAway',
  'Funding Societies','Kredivo','Sea Limited / Shopee','Lazada',
  'ByteDance Singapore','TikTok','Stripe Singapore','MoMo Vietnam',
  'VNG Corporation','Carro','Atome',
]);

const DUBAI_COMPANIES = new Set([
  'Careem','Property Finder','Tamara','Bayzat','Kitopi','Deliveroo MENA',
  'HubSpot MENA','Tabby','Dubizzle / Bayut','Noon','Talabat','Wio Bank',
  'Mastercard Dubai','Salesforce UAE','SAP UAE','Emirates NBD Digital','Namshi',
]);

const INDIA_KW  = ['india','bengaluru','bangalore','mumbai','delhi','hyderabad','chennai','pune','noida','gurgaon','gurugram'];
const ASEAN_KW  = ['singapore','indonesia','jakarta','vietnam','ho chi minh','hanoi','malaysia','kuala lumpur','philippines','manila','thailand','bangkok'];
const DUBAI_KW  = ['dubai','uae','abu dhabi','riyadh','saudi','mena','middle east'];

function getRegion(companyName, location) {
  const loc = (location || '').toLowerCase();
  if (INDIA_KW.some(k => loc.includes(k)))  return 'india';
  if (DUBAI_KW.some(k => loc.includes(k)))  return 'dubai';
  if (ASEAN_KW.some(k => loc.includes(k)))  return 'asean';
  if (INDIA_COMPANIES.has(companyName))      return 'india';
  if (ASEAN_COMPANIES.has(companyName))      return 'asean';
  if (DUBAI_COMPANIES.has(companyName))      return 'dubai';
  return 'global';
}

// ── API detection ──────────────────────────────────────────────────────────────

function detectApi(company) {
  if (company.api && company.api.includes('greenhouse')) {
    const base = company.api.replace(/\?.*$/, '');
    return { type: 'greenhouse', url: base + '?content=true' };
  }
  const url = company.careers_url || '';

  const ashby = url.match(/jobs\.ashbyhq\.com\/([^/?#]+)/);
  if (ashby) return { type: 'ashby', url: `https://api.ashbyhq.com/posting-api/job-board/${ashby[1]}?includeCompensation=true` };

  const lever = url.match(/jobs\.lever\.co\/([^/?#]+)/);
  if (lever) return { type: 'lever', url: `https://api.lever.co/v0/postings/${lever[1]}` };

  const gh = url.match(/job-boards(?:\.eu)?\.greenhouse\.io\/([^/?#]+)/);
  if (gh) return { type: 'greenhouse', url: `https://boards-api.greenhouse.io/v1/boards/${gh[1]}/jobs?content=true` };

  return null;
}

// ── Parsers ────────────────────────────────────────────────────────────────────

function parseGreenhouse(json, company) {
  return (json.jobs || []).map(j => ({
    title:       j.title || '',
    url:         j.absolute_url || '',
    company:     company.name,
    location:    j.location?.name || '',
    description: j.content || '',
    source:      'Greenhouse',
  }));
}

function parseAshby(json, company) {
  return (json.jobs || []).map(j => ({
    title:       j.title || '',
    url:         j.jobUrl || '',
    company:     company.name,
    location:    j.location || '',
    description: j.descriptionHtml || '',
    source:      'Ashby',
  }));
}

function parseLever(json, company) {
  if (!Array.isArray(json)) return [];
  return json.map(j => ({
    title:       j.text || '',
    url:         j.hostedUrl || '',
    company:     company.name,
    location:    j.categories?.location || '',
    description: j.description || '',
    source:      'Lever',
  }));
}

const PARSERS = { greenhouse: parseGreenhouse, ashby: parseAshby, lever: parseLever };

// ── Title filter ───────────────────────────────────────────────────────────────

function buildFilter(cfg) {
  const pos = (cfg?.positive || []).map(k => k.toLowerCase());
  const neg = (cfg?.negative || []).map(k => k.toLowerCase());
  return title => {
    const t = title.toLowerCase();
    return (pos.length === 0 || pos.some(k => t.includes(k))) && !neg.some(k => t.includes(k));
  };
}

// ── Fetch helpers ──────────────────────────────────────────────────────────────

async function fetchJson(url) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(url, { signal: ctrl.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } finally {
    clearTimeout(timer);
  }
}

async function parallel(tasks, limit) {
  let i = 0;
  async function next() { while (i < tasks.length) await tasks[i++](); }
  await Promise.all(Array.from({ length: Math.min(limit, tasks.length) }, next));
}

// ── Job fetch ──────────────────────────────────────────────────────────────────

async function fetchJobs(config) {
  const filter = buildFilter(config.title_filter);
  const targets = (config.tracked_companies || [])
    .filter(c => c.enabled !== false)
    .map(c => ({ ...c, _api: detectApi(c) }))
    .filter(c => c._api !== null);

  const jobs = [];
  const errors = [];

  await parallel(targets.map(co => async () => {
    try {
      const json = await fetchJson(co._api.url);
      for (const job of PARSERS[co._api.type](json, co)) {
        if (filter(job.title)) {
          jobs.push({ ...job, region: getRegion(co.name, job.location) });
        }
      }
    } catch (e) {
      errors.push(`${co.name}: ${e.message}`);
    }
  }), CONCURRENCY);

  if (errors.length) console.warn(`  ${errors.length} fetch errors:`, errors.slice(0, 5).join(', '));

  const ORDER = { india: 0, asean: 1, dubai: 2, global: 3 };
  return jobs.sort((a, b) => (ORDER[a.region] ?? 3) - (ORDER[b.region] ?? 3));
}

// ── Cache ──────────────────────────────────────────────────────────────────────

let _cache = null;
let _cacheAt = 0;

async function getJobs(config, force = false) {
  if (!force && _cache && Date.now() - _cacheAt < CACHE_TTL_MS) return _cache;
  console.log(`[${new Date().toLocaleTimeString()}] Fetching jobs from APIs...`);
  _cache = await fetchJobs(config);
  _cacheAt = Date.now();
  console.log(`[${new Date().toLocaleTimeString()}] ${_cache.length} matching jobs cached.`);
  return _cache;
}

// ── HTML (single-file SPA) ─────────────────────────────────────────────────────

const HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PMM Jobs — India · ASEAN · Dubai</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#f3f4f6;min-height:100vh;color:#111827}
/* Header */
.hdr{background:linear-gradient(135deg,#1e1b4b 0%,#4338ca 100%);color:#fff;padding:22px 32px;display:flex;align-items:center;gap:16px}
.hdr-text h1{font-size:20px;font-weight:700;letter-spacing:-.3px}
.hdr-text p{color:#a5b4fc;font-size:13px;margin-top:3px}
.hdr-stats{margin-left:auto;display:flex;gap:20px}
.stat{text-align:right}
.stat-n{font-size:22px;font-weight:700;color:#c7d2fe}
.stat-l{font-size:11px;color:#818cf8;text-transform:uppercase;letter-spacing:.5px}
/* Filters */
.bar{background:#fff;border-bottom:1px solid #e5e7eb;padding:12px 32px;display:flex;gap:8px;align-items:center;position:sticky;top:0;z-index:20;flex-wrap:wrap}
.tab{padding:6px 16px;border-radius:20px;border:1.5px solid #e5e7eb;background:#fff;cursor:pointer;font-size:13px;font-weight:500;color:#6b7280;transition:all .15s;white-space:nowrap}
.tab:hover{border-color:#6366f1;color:#6366f1}
.tab.on{background:#6366f1;color:#fff;border-color:#6366f1}
.bar-right{margin-left:auto;display:flex;gap:8px;align-items:center}
.srch{padding:7px 14px;border:1.5px solid #e5e7eb;border-radius:20px;font-size:13px;width:210px;outline:none;transition:border .15s}
.srch:focus{border-color:#6366f1}
.rfbtn{padding:6px 14px;border:1.5px solid #e5e7eb;border-radius:20px;background:#fff;cursor:pointer;font-size:13px;color:#6b7280;transition:all .15s}
.rfbtn:hover{border-color:#6366f1;color:#6366f1}
.rfbtn.spinning{pointer-events:none;opacity:.5}
/* Main */
.main{padding:22px 32px}
.meta{color:#6b7280;font-size:13px;margin-bottom:14px;display:flex;align-items:center;gap:12px}
.cache-age{color:#9ca3af;font-size:12px}
/* Grid */
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:13px}
/* Card */
.card{background:#fff;border:1.5px solid #e5e7eb;border-radius:12px;padding:18px 20px;cursor:pointer;transition:all .18s;display:flex;flex-direction:column;gap:10px}
.card:hover{border-color:#6366f1;box-shadow:0 4px 20px rgba(99,102,241,.13);transform:translateY(-2px)}
.c-top{display:flex;align-items:center;gap:10px}
.av{width:38px;height:38px;border-radius:9px;display:flex;align-items:center;justify-content:center;font-weight:800;font-size:15px;color:#fff;flex-shrink:0}
.c-co{font-size:12px;color:#9ca3af;font-weight:600;text-transform:uppercase;letter-spacing:.5px}
.c-ti{font-size:14px;font-weight:600;color:#111827;line-height:1.4}
.c-ft{display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-top:2px}
.bdg{padding:2px 9px;border-radius:10px;font-size:11px;font-weight:700;white-space:nowrap}
.bi{background:#fff3e0;color:#c2410c}
.ba{background:#dcfce7;color:#15803d}
.bd{background:#fef9c3;color:#a16207}
.bg{background:#ede9fe;color:#6d28d9}
.loc{font-size:11px;color:#9ca3af}
.ats{font-size:10px;color:#d1d5db;margin-left:auto;text-transform:uppercase;letter-spacing:.5px;font-weight:600}
/* Overlay */
.ov{display:none;position:fixed;inset:0;background:rgba(17,24,39,.5);z-index:50;backdrop-filter:blur(2px)}
.ov.open{display:flex;justify-content:flex-end}
/* Drawer */
.dw{background:#fff;width:min(720px,100vw);height:100vh;overflow-y:auto;display:flex;flex-direction:column;box-shadow:-20px 0 60px rgba(0,0,0,.15)}
.dh{padding:20px 28px;border-bottom:1px solid #f3f4f6;position:sticky;top:0;background:#fff;z-index:1}
.dbk{display:flex;align-items:center;gap:5px;color:#9ca3af;font-size:13px;cursor:pointer;background:none;border:none;padding:0;margin-bottom:12px;transition:color .15s}
.dbk:hover{color:#6366f1}
.dco{font-size:11px;color:#9ca3af;font-weight:700;text-transform:uppercase;letter-spacing:.8px;margin-bottom:4px}
.dti{font-size:19px;font-weight:700;color:#111827;line-height:1.3;margin-bottom:10px}
.dme{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.db{padding:28px;flex:1}
.jd{font-size:14px;line-height:1.8;color:#374151}
.jd h1,.jd h2,.jd h3{font-size:15px;font-weight:700;color:#111827;margin:18px 0 7px}
.jd h1:first-child,.jd h2:first-child{margin-top:0}
.jd p{margin-bottom:11px}
.jd ul,.jd ol{margin:6px 0 12px 20px}
.jd li{margin-bottom:4px}
.jd a{color:#6366f1}
.df{padding:18px 28px;border-top:1px solid #f3f4f6;position:sticky;bottom:0;background:#fff}
.abtn{display:flex;align-items:center;justify-content:center;gap:8px;width:100%;padding:13px;background:#6366f1;color:#fff;text-decoration:none;border-radius:10px;font-weight:600;font-size:15px;transition:background .15s}
.abtn:hover{background:#4f46e5}
/* States */
.load{display:flex;flex-direction:column;align-items:center;padding:80px;color:#9ca3af;gap:14px}
.spin{width:30px;height:30px;border:3px solid #e5e7eb;border-top-color:#6366f1;border-radius:50%;animation:sp .7s linear infinite}
@keyframes sp{to{transform:rotate(360deg)}}
.empty{text-align:center;padding:80px;color:#9ca3af;font-size:15px}
</style>
</head>
<body>

<div class="hdr">
  <div class="hdr-text">
    <h1>&#127919; PMM Jobs Portal</h1>
    <p>India &middot; ASEAN &middot; Dubai &mdash; live from company portals</p>
  </div>
  <div class="hdr-stats" id="hstats"></div>
</div>

<div class="bar">
  <button class="tab on" data-r="all">All</button>
  <button class="tab" data-r="india">&#127470;&#127475; India</button>
  <button class="tab" data-r="asean">&#127758; ASEAN</button>
  <button class="tab" data-r="dubai">&#127462;&#127466; Dubai</button>
  <button class="tab" data-r="global">&#127760; Global</button>
  <div class="bar-right">
    <input class="srch" id="q" type="text" placeholder="Search title or company&#8230;">
    <button class="rfbtn" id="rf">&#8635; Refresh</button>
  </div>
</div>

<div class="main">
  <div class="meta" id="meta"></div>
  <div class="grid" id="grid">
    <div class="load"><div class="spin"></div><span>Fetching from company portals&#8230;</span></div>
  </div>
</div>

<div class="ov" id="ov">
  <div class="dw" id="dw">
    <div class="dh">
      <button class="dbk" id="cls">&#8592; Back to listings</button>
      <div class="dco" id="dco"></div>
      <div class="dti" id="dti"></div>
      <div class="dme" id="dme"></div>
    </div>
    <div class="db"><div class="jd" id="djd"></div></div>
    <div class="df"><a class="abtn" id="dap" href="#" target="_blank" rel="noopener">Apply on company site &#8594;</a></div>
  </div>
</div>

<script>
var COLORS=['#4F46E5','#7C3AED','#0891B2','#059669','#DC2626','#D97706','#BE185D','#0F766E','#B45309','#4338CA'];
var cc={},ci=0;
function col(c){if(!cc[c]){cc[c]=COLORS[ci%COLORS.length];ci++;}return cc[c];}

function badge(r){
  var m={india:'<span class="bdg bi">&#127470;&#127475; India</span>',asean:'<span class="bdg ba">&#127758; ASEAN</span>',dubai:'<span class="bdg bd">&#127462;&#127466; Dubai</span>',global:'<span class="bdg bg">&#127760; Global</span>'};
  return m[r]||m['global'];
}

var all=[], region='all', q='', cacheTs=null;

function visible(){
  return all.filter(function(j){
    var rm=region==='all'||j.region===region;
    var sm=!q||j.title.toLowerCase().indexOf(q)>-1||j.company.toLowerCase().indexOf(q)>-1||(j.location||'').toLowerCase().indexOf(q)>-1;
    return rm&&sm;
  });
}

function counts(){
  var c={india:0,asean:0,dubai:0,global:0};
  all.forEach(function(j){c[j.region]=(c[j.region]||0)+1;});
  return c;
}

function renderStats(){
  var c=counts();
  var hs=document.getElementById('hstats');
  hs.innerHTML='<div class="stat"><div class="stat-n">'+all.length+'</div><div class="stat-l">Total</div></div>'+
    '<div class="stat"><div class="stat-n">'+c.india+'</div><div class="stat-l">India</div></div>'+
    '<div class="stat"><div class="stat-n">'+c.asean+'</div><div class="stat-l">ASEAN</div></div>'+
    '<div class="stat"><div class="stat-n">'+c.dubai+'</div><div class="stat-l">Dubai</div></div>';
}

function render(){
  var jobs=visible();
  var grid=document.getElementById('grid');
  var meta=document.getElementById('meta');

  if(!jobs.length){
    grid.innerHTML='<div class="empty">No matching jobs found.</div>';
    meta.textContent='';
    return;
  }

  meta.innerHTML=jobs.length+' job'+(jobs.length!==1?'s':'')+' found'+(cacheTs?' &nbsp;<span class="cache-age">cached '+cacheTs+'</span>':'');

  grid.innerHTML=jobs.map(function(j,i){
    return '<div class="card" onclick="open('+i+')">'+
      '<div class="c-top"><div class="av" style="background:'+col(j.company)+'">'+j.company[0].toUpperCase()+'</div>'+
      '<div class="c-co">'+esc(j.company)+'</div></div>'+
      '<div class="c-ti">'+esc(j.title)+'</div>'+
      '<div class="c-ft">'+badge(j.region)+
      '<span class="loc">&#128205; '+esc(j.location||'Remote / N/A')+'</span>'+
      '<span class="ats">'+j.source+'</span></div></div>';
  }).join('');
}

function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}

function open(i){
  var j=visible()[i];
  document.getElementById('dco').textContent=j.company;
  document.getElementById('dti').textContent=j.title;
  document.getElementById('dme').innerHTML=badge(j.region)+' <span class="loc" style="font-size:13px">&#128205; '+(j.location||'N/A')+'</span>';
  document.getElementById('djd').innerHTML=j.description||'<p style="color:#9ca3af">Description not available — click Apply to view the full posting on company site.</p>';
  document.getElementById('dap').href=j.url;
  document.getElementById('ov').classList.add('open');
  document.getElementById('dw').scrollTop=0;
}

document.getElementById('cls').onclick=function(){document.getElementById('ov').classList.remove('open');};
document.getElementById('ov').onclick=function(e){if(e.target===this)this.classList.remove('open');};

document.querySelectorAll('.tab').forEach(function(t){
  t.onclick=function(){
    document.querySelector('.tab.on').classList.remove('on');
    t.classList.add('on');
    region=t.dataset.r;
    render();
  };
});

document.getElementById('q').oninput=function(){q=this.value.toLowerCase().trim();render();};

function timeSince(ms){
  var s=Math.round((Date.now()-ms)/1000);
  if(s<60)return s+'s ago';
  return Math.round(s/60)+'m ago';
}

function relTime(){
  if(!cacheTs)return;
  document.getElementById('meta').innerHTML=visible().length+' job'+(visible().length!==1?'s':'')+' found &nbsp;<span class="cache-age">cached '+cacheTs+'</span>';
}

var _cacheEpoch=null;
setInterval(function(){
  if(_cacheEpoch){cacheTs=timeSince(_cacheEpoch);relTime();}
},15000);

async function load(force){
  var btn=document.getElementById('rf');
  btn.classList.add('spinning');
  btn.textContent='Loading...';
  document.getElementById('grid').innerHTML='<div class="load"><div class="spin"></div><span>Fetching from company portals&#8230;</span></div>';
  document.getElementById('meta').textContent='';
  document.getElementById('hstats').innerHTML='';
  try{
    var r=await fetch('/api/jobs'+(force?'?refresh=1':''));
    var d=await r.json();
    all=d.jobs||[];
    _cacheEpoch=Date.now();
    cacheTs='just now';
    renderStats();
    render();
  }catch(e){
    document.getElementById('grid').innerHTML='<div class="empty">Error loading jobs. Is the server running?</div>';
  }finally{
    btn.classList.remove('spinning');
    btn.textContent='&#8635; Refresh';
  }
}

document.getElementById('rf').onclick=function(){load(true);};
load();
</script>
</body>
</html>`;

// ── HTTP server ────────────────────────────────────────────────────────────────

async function main() {
  if (!existsSync(PORTALS_PATH)) {
    console.error('portals.yml not found. Run this from the career-ops directory.');
    process.exit(1);
  }

  const config = yaml.load(readFileSync(PORTALS_PATH, 'utf-8'));

  const server = createServer(async (req, res) => {
    const url = new URL(req.url, `http://localhost:${PORT}`);

    if (url.pathname === '/api/jobs') {
      const force = url.searchParams.has('refresh');
      try {
        const jobs = await getJobs(config, force);
        res.writeHead(200, {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*',
          'Cache-Control': 'no-store',
        });
        res.end(JSON.stringify({ jobs, count: jobs.length, timestamp: new Date().toISOString() }));
      } catch (err) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      }
      return;
    }

    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(HTML);
  });

  server.listen(PORT, () => {
    console.log('\n\x1b[35m\x1b[1m🎯 PMM Jobs Portal\x1b[0m');
    console.log(`   \x1b[36mhttp://localhost:${PORT}\x1b[0m`);
    console.log('   India · ASEAN · Dubai · Global');
    console.log('   Cache TTL: 30 min · Press Ctrl+C to stop\n');
  });

  // Pre-warm cache in background
  getJobs(config).catch(e => console.warn('Pre-warm error:', e.message));
}

main().catch(e => { console.error('Fatal:', e.message); process.exit(1); });
