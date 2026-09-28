const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const url = require('url');
const { Pool } = require('pg');

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';
const ADMIN_KEY = process.env.ADMIN_KEY || 'change-me-before-deploy';
const ROOT = __dirname;
const DATABASE_URL = String(process.env.DATABASE_URL || '').trim();
const USE_POSTGRES = !!DATABASE_URL;

const DATA_DIR = process.env.DATA_DIR || path.join(ROOT, 'data');
const PARTICIPANTS_FILE = path.join(DATA_DIR, 'participants.json');
const RESULTS_FILE = path.join(DATA_DIR, 'results.json');
const PERIODS_FILE = path.join(DATA_DIR, 'periods.json');
if (!USE_POSTGRES) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  for (const f of [PARTICIPANTS_FILE, RESULTS_FILE, PERIODS_FILE]) if (!fs.existsSync(f)) fs.writeFileSync(f, '[]', 'utf8');
}

const pool = USE_POSTGRES ? new Pool({
  connectionString: DATABASE_URL,
  max: Number(process.env.PG_POOL_MAX || 10),
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
  ssl: process.env.PGSSLMODE === 'require' ? { rejectUnauthorized: false } : undefined
}) : null;

const MIME = {'.html':'text/html; charset=utf-8','.js':'application/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.json':'application/json; charset=utf-8','.png':'image/png','.jpg':'image/jpeg','.jpeg':'image/jpeg','.svg':'image/svg+xml','.ico':'image/x-icon'};
const AXIS_KEYS = ['OF','RP','CS','LE'];
const SCORE_VALUES = new Set([3,2,1,-1,-2,-3]);
const CHOICE_VALUES = new Set([1,2,3,4,5,6]);

function readJson(f){try{return JSON.parse(fs.readFileSync(f,'utf8'))}catch{return []}}
function writeJson(f,d){const t=f+'.tmp';fs.writeFileSync(t,JSON.stringify(d,null,2),'utf8');fs.renameSync(t,f)}
function send(res,status,obj,headers={}){const body=Buffer.from(typeof obj==='string'?obj:JSON.stringify(obj));res.writeHead(status,{'Content-Type':typeof obj==='string'?'text/plain; charset=utf-8':'application/json; charset=utf-8','Content-Length':body.length,'Cache-Control':'no-store',...headers});res.end(body)}
function clean(v,max=100){return String(v??'').trim().slice(0,max)}
function normalized(v){return clean(v,200).normalize('NFKC').toLowerCase().replace(/\s+/g,' ').trim()}
function participantKey(team,name,employeeId){const eid=normalized(employeeId);return eid?`emp:${eid}`:`person:${normalized(team)}\u0000${normalized(name)}`}
function isAdmin(req){const got=Buffer.from(String(req.headers['x-admin-key']||''));const want=Buffer.from(String(ADMIN_KEY));return got.length===want.length && crypto.timingSafeEqual(got,want)}
function bodyJson(req){return new Promise((resolve,reject)=>{let d='';req.on('data',c=>{d+=c;if(d.length>2_000_000){reject(new Error('payload too large'));req.destroy()}});req.on('end',()=>{try{resolve(d?JSON.parse(d):{})}catch(e){reject(e)}});req.on('error',reject)})}
function periodLabel(year,half){return `${year}년 ${Number(half)===1?'상반기':'하반기'}`}
function currentDefaultPeriod(){const d=new Date();const year=d.getFullYear();const half=(d.getMonth()+1)<=6?1:2;return {year,half,label:periodLabel(year,half)}}
function dbRowParticipant(r){return {id:r.id,team:r.team,name:r.name,employeeId:r.employee_id||''}}
function dbRowPeriod(r){return {id:r.id,year:Number(r.year),half:Number(r.half),label:r.label,isActive:!!r.is_active,createdAt:r.created_at instanceof Date?r.created_at.toISOString():String(r.created_at||'')}}
function dbRowResult(r){return {participantId:r.participant_id,periodId:r.period_id,periodLabel:r.period_label||'',periodYear:Number(r.period_year||0),periodHalf:Number(r.period_half||0),team:r.team,name:r.name,employeeId:r.employee_id||'',answerScores:r.answer_scores||[],choiceNumbers:r.choice_numbers||[],axisScores:r.axis_scores||{},totalScore:Number(r.total_score||0),type:r.type||'',typeName:r.type_name||'',submittedAt:r.submitted_at instanceof Date?r.submitted_at.toISOString():String(r.submitted_at||'')}}
function validateResult(b){
  const scores=b.answerScores,choices=b.choiceNumbers;
  if(!Array.isArray(scores)||scores.length!==24||!scores.every(v=>SCORE_VALUES.has(Number(v)))) return '핵심가치 문항 응답 형식이 올바르지 않습니다.';
  if(!Array.isArray(choices)||choices.length!==24||!choices.every(v=>CHOICE_VALUES.has(Number(v)))) return '선택번호 형식이 올바르지 않습니다.';
  const expectedChoices=scores.map(v=>({3:1,2:2,1:3,'-1':4,'-2':5,'-3':6})[Number(v)]);
  if(expectedChoices.some((v,i)=>v!==Number(choices[i]))) return '선택번호와 환산값이 일치하지 않습니다.';
  const axis={OF:0,RP:0,CS:0,LE:0};
  scores.map(Number).forEach((v,i)=>{axis[AXIS_KEYS[Math.floor(i/6)]]+=v});
  const given=b.axisScores||{};
  if(AXIS_KEYS.some(k=>Number(given[k])!==axis[k])) return '축 점수 검증에 실패했습니다.';
  const expectedType=(axis.OF>=0?'O':'F')+(axis.RP>=0?'R':'P')+(axis.CS>=0?'C':'S')+(axis.LE>=0?'L':'E');
  if(clean(b.type,10)!==expectedType) return '유형 결과 검증에 실패했습니다.';
  const total=Number(b.totalScore);
  if(!Number.isFinite(total)||total<0||total>100) return '총점 형식이 올바르지 않습니다.';
  return null;
}

async function initDb(){
  if(!USE_POSTGRES)return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS participants (
      id UUID PRIMARY KEY,
      participant_key TEXT NOT NULL UNIQUE,
      team TEXT NOT NULL,
      name TEXT NOT NULL,
      employee_id TEXT NOT NULL DEFAULT '',
      active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_participants_active_team_name ON participants(active, team, name);
    CREATE TABLE IF NOT EXISTS diagnosis_periods (
      id UUID PRIMARY KEY,
      year INTEGER NOT NULL,
      half SMALLINT NOT NULL CHECK (half IN (1,2)),
      label TEXT NOT NULL,
      is_active BOOLEAN NOT NULL DEFAULT FALSE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(year, half)
    );
  `);
  let periods=await pool.query(`SELECT * FROM diagnosis_periods ORDER BY year,half`);
  if(!periods.rowCount){const p=currentDefaultPeriod();await pool.query(`INSERT INTO diagnosis_periods(id,year,half,label,is_active) VALUES($1,$2,$3,$4,TRUE)`,[crypto.randomUUID(),p.year,p.half,p.label]);}
  else if(!periods.rows.some(r=>r.is_active)){await pool.query(`UPDATE diagnosis_periods SET is_active=TRUE WHERE id=(SELECT id FROM diagnosis_periods ORDER BY year DESC,half DESC LIMIT 1)`)}
  await pool.query(`
    CREATE TABLE IF NOT EXISTS core_value_results (
      participant_id UUID NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
      period_id UUID REFERENCES diagnosis_periods(id),
      answer_scores JSONB NOT NULL,
      choice_numbers JSONB NOT NULL,
      axis_scores JSONB NOT NULL,
      total_score INTEGER NOT NULL,
      type VARCHAR(10) NOT NULL,
      type_name TEXT NOT NULL,
      submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    ALTER TABLE core_value_results ADD COLUMN IF NOT EXISTS period_id UUID REFERENCES diagnosis_periods(id);
  `);
  const active=await pool.query(`SELECT id FROM diagnosis_periods WHERE is_active=TRUE LIMIT 1`);
  await pool.query(`UPDATE core_value_results SET period_id=$1 WHERE period_id IS NULL`,[active.rows[0].id]);
  await pool.query(`ALTER TABLE core_value_results ALTER COLUMN period_id SET NOT NULL`);
  await pool.query(`ALTER TABLE core_value_results DROP CONSTRAINT IF EXISTS core_value_results_pkey`);
  await pool.query(`ALTER TABLE core_value_results ADD PRIMARY KEY (participant_id, period_id)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_core_value_results_period_submitted ON core_value_results(period_id,submitted_at DESC)`);
}

async function ensureLocalPeriod(){
  let ps=readJson(PERIODS_FILE);
  if(!ps.length){const p=currentDefaultPeriod();ps=[{id:crypto.randomUUID(),...p,isActive:true,createdAt:new Date().toISOString()}];writeJson(PERIODS_FILE,ps)}
  if(!ps.some(p=>p.isActive)){ps.sort((a,b)=>b.year-a.year||b.half-a.half);ps[0].isActive=true;writeJson(PERIODS_FILE,ps)}
  return ps;
}
async function getPeriods(){
  if(USE_POSTGRES){const {rows}=await pool.query(`SELECT id,year,half,label,is_active,created_at FROM diagnosis_periods ORDER BY year DESC,half DESC`);return rows.map(dbRowPeriod)}
  return await ensureLocalPeriod();
}
async function getActivePeriod(){const ps=await getPeriods();return ps.find(p=>p.isActive)||null}
async function createOrActivatePeriod(year,half,activate=true){
  year=Number(year);half=Number(half);if(!Number.isInteger(year)||year<2020||year>2100||![1,2].includes(half))throw new Error('년도/반기 값이 올바르지 않습니다.');
  const label=periodLabel(year,half);
  if(USE_POSTGRES){
    const client=await pool.connect();try{await client.query('BEGIN');if(activate)await client.query(`UPDATE diagnosis_periods SET is_active=FALSE WHERE is_active=TRUE`);const {rows}=await client.query(`INSERT INTO diagnosis_periods(id,year,half,label,is_active) VALUES($1,$2,$3,$4,$5) ON CONFLICT(year,half) DO UPDATE SET label=EXCLUDED.label,is_active=EXCLUDED.is_active RETURNING id,year,half,label,is_active,created_at`,[crypto.randomUUID(),year,half,label,activate]);await client.query('COMMIT');return dbRowPeriod(rows[0])}catch(e){await client.query('ROLLBACK');throw e}finally{client.release()}
  }
  let ps=await ensureLocalPeriod();if(activate)ps=ps.map(p=>({...p,isActive:false}));let p=ps.find(p=>Number(p.year)===year&&Number(p.half)===half);if(p){p.label=label;p.isActive=activate}else{p={id:crypto.randomUUID(),year,half,label,isActive:activate,createdAt:new Date().toISOString()};ps.push(p)}writeJson(PERIODS_FILE,ps);return p;
}
async function activatePeriod(periodId){
  if(USE_POSTGRES){const client=await pool.connect();try{await client.query('BEGIN');const found=await client.query(`SELECT id FROM diagnosis_periods WHERE id=$1`,[periodId]);if(!found.rowCount)throw new Error('진단 기간을 찾을 수 없습니다.');await client.query(`UPDATE diagnosis_periods SET is_active=FALSE WHERE is_active=TRUE`);await client.query(`UPDATE diagnosis_periods SET is_active=TRUE WHERE id=$1`,[periodId]);await client.query('COMMIT')}catch(e){await client.query('ROLLBACK');throw e}finally{client.release()}return}
  let ps=await ensureLocalPeriod();if(!ps.some(p=>p.id===periodId))throw new Error('진단 기간을 찾을 수 없습니다.');ps=ps.map(p=>({...p,isActive:p.id===periodId}));writeJson(PERIODS_FILE,ps);
}

async function publicParticipants(){
  const activePeriod=await getActivePeriod();
  if(USE_POSTGRES){const {rows}=await pool.query(`SELECT id,team,name FROM participants WHERE active=TRUE ORDER BY team,name`);return {participants:rows.map(r=>({id:r.id,team:r.team,name:r.name})),activePeriod}}
  return {participants:readJson(PARTICIPANTS_FILE).filter(p=>p.active!==false).map(({id,team,name})=>({id,team,name})),activePeriod};
}
async function resultHistory(participantId){
  participantId=clean(participantId,60);
  if(USE_POSTGRES){const {rows}=await pool.query(`SELECT r.participant_id,r.period_id,d.label period_label,d.year period_year,d.half period_half,p.team,p.name,'' employee_id,r.answer_scores,r.choice_numbers,r.axis_scores,r.total_score,r.type,r.type_name,r.submitted_at FROM core_value_results r JOIN participants p ON p.id=r.participant_id JOIN diagnosis_periods d ON d.id=r.period_id WHERE r.participant_id=$1 AND p.active=TRUE ORDER BY d.year,d.half`,[participantId]);return rows.map(dbRowResult).map(({answerScores,choiceNumbers,employeeId,...x})=>x)}
  const ps=await getPeriods();const pm=new Map(ps.map(p=>[p.id,p]));return readJson(RESULTS_FILE).filter(r=>r.participantId===participantId).map(r=>{const p=pm.get(r.periodId)||{};const {answerScores,choiceNumbers,employeeId,...x}=r;return {...x,periodLabel:p.label||r.periodLabel||'',periodYear:p.year||r.periodYear||0,periodHalf:p.half||r.periodHalf||0}}).sort((a,b)=>a.periodYear-b.periodYear||a.periodHalf-b.periodHalf);
}
async function adminData(){
  const periods=await getPeriods();
  if(USE_POSTGRES){
    const [p,r]=await Promise.all([
      pool.query(`SELECT id,team,name,employee_id FROM participants WHERE active=TRUE ORDER BY team,name`),
      pool.query(`SELECT r.participant_id,r.period_id,d.label period_label,d.year period_year,d.half period_half,p.team,p.name,p.employee_id,r.answer_scores,r.choice_numbers,r.axis_scores,r.total_score,r.type,r.type_name,r.submitted_at FROM core_value_results r JOIN participants p ON p.id=r.participant_id JOIN diagnosis_periods d ON d.id=r.period_id WHERE p.active=TRUE ORDER BY d.year DESC,d.half DESC,p.team,p.name`)
    ]);return {participants:p.rows.map(dbRowParticipant),results:r.rows.map(dbRowResult),periods,activePeriod:periods.find(x=>x.isActive)||null};
  }
  const pm=new Map(periods.map(p=>[p.id,p]));const results=readJson(RESULTS_FILE).map(r=>{const p=pm.get(r.periodId)||{};return {...r,periodLabel:p.label||r.periodLabel||'',periodYear:p.year||r.periodYear||0,periodHalf:p.half||r.periodHalf||0}});return {participants:readJson(PARTICIPANTS_FILE).filter(p=>p.active!==false),results,periods,activePeriod:periods.find(x=>x.isActive)||null};
}
async function replaceRoster(input){
  const cleaned=[];const seen=new Set();for(const row of input){const team=clean(row.team),name=clean(row.name),employeeId=clean(row.employeeId,50);if(!team||!name)continue;const key=participantKey(team,name,employeeId);if(seen.has(key))continue;seen.add(key);cleaned.push({team,name,employeeId,key})}
  if(USE_POSTGRES){const client=await pool.connect();try{await client.query('BEGIN');await client.query('UPDATE participants SET active=FALSE,updated_at=NOW() WHERE active=TRUE');for(const p of cleaned){await client.query(`INSERT INTO participants(id,participant_key,team,name,employee_id,active) VALUES($1,$2,$3,$4,$5,TRUE) ON CONFLICT(participant_key) DO UPDATE SET team=EXCLUDED.team,name=EXCLUDED.name,employee_id=EXCLUDED.employee_id,active=TRUE,updated_at=NOW()`,[crypto.randomUUID(),p.key,p.team,p.name,p.employeeId])}await client.query('COMMIT')}catch(e){await client.query('ROLLBACK');throw e}finally{client.release()}return cleaned.length}
  const old=readJson(PARTICIPANTS_FILE);const oldByKey=new Map(old.map(p=>[p.key||participantKey(p.team,p.name,p.employeeId),p]));const participants=cleaned.map(p=>({id:oldByKey.get(p.key)?.id||crypto.randomUUID(),key:p.key,team:p.team,name:p.name,employeeId:p.employeeId,active:true}));writeJson(PARTICIPANTS_FILE,participants);return participants.length;
}
async function saveResult(b){
  const validation=validateResult(b);if(validation)return {error:validation,status:400};const active=await getActivePeriod();if(!active)return {error:'활성 진단 기간이 설정되지 않았습니다.',status:400};
  if(USE_POSTGRES){const p=await pool.query(`SELECT id FROM participants WHERE id=$1 AND active=TRUE`,[clean(b.participantId,60)]);if(!p.rowCount)return {error:'등록되지 않은 직원입니다.',status:400};await pool.query(`INSERT INTO core_value_results(participant_id,period_id,answer_scores,choice_numbers,axis_scores,total_score,type,type_name,submitted_at,updated_at) VALUES($1,$2,$3::jsonb,$4::jsonb,$5::jsonb,$6,$7,$8,NOW(),NOW()) ON CONFLICT(participant_id,period_id) DO UPDATE SET answer_scores=EXCLUDED.answer_scores,choice_numbers=EXCLUDED.choice_numbers,axis_scores=EXCLUDED.axis_scores,total_score=EXCLUDED.total_score,type=EXCLUDED.type,type_name=EXCLUDED.type_name,submitted_at=NOW(),updated_at=NOW()`,[b.participantId,active.id,JSON.stringify(b.answerScores.map(Number)),JSON.stringify(b.choiceNumbers.map(Number)),JSON.stringify(b.axisScores),Math.round(Number(b.totalScore)),clean(b.type,10),clean(b.typeName,100)]);return {ok:true,period:active}}
  const participants=readJson(PARTICIPANTS_FILE),p=participants.find(x=>x.id===b.participantId&&x.active!==false);if(!p)return {error:'등록되지 않은 직원입니다.',status:400};const result={participantId:p.id,periodId:active.id,periodLabel:active.label,periodYear:active.year,periodHalf:active.half,team:p.team,name:p.name,employeeId:p.employeeId||'',answerScores:b.answerScores.map(Number),choiceNumbers:b.choiceNumbers.map(Number),axisScores:b.axisScores,totalScore:Math.round(Number(b.totalScore)),type:clean(b.type,10),typeName:clean(b.typeName,100),submittedAt:new Date().toISOString()};let results=readJson(RESULTS_FILE);const i=results.findIndex(x=>x.participantId===p.id&&x.periodId===active.id);if(i>=0)results[i]=result;else results.push(result);writeJson(RESULTS_FILE,results);return {ok:true,period:active};
}

async function api(req,res,pathname){
  if(req.method==='GET'&&pathname==='/api/participants')return send(res,200,await publicParticipants());
  if(req.method==='GET'&&pathname==='/api/results/history'){const q=url.parse(req.url,true).query;const participantId=clean(q.participantId,60);if(!participantId)return send(res,400,{error:'직원을 선택해 주세요.'});return send(res,200,{results:await resultHistory(participantId),activePeriod:await getActivePeriod()})}
  if(pathname.startsWith('/api/admin/')&&!isAdmin(req))return send(res,401,{error:'관리자 인증에 실패했습니다.'});
  if(req.method==='GET'&&pathname==='/api/admin/status'){let dbOk=true;if(USE_POSTGRES){try{await pool.query('SELECT 1')}catch{dbOk=false}}return send(res,200,{storage:USE_POSTGRES?'PostgreSQL':'JSON (local test only)',databaseConnected:dbOk,activePeriod:await getActivePeriod()})}
  if(req.method==='POST'&&pathname==='/api/admin/participants'){const b=await bodyJson(req);const input=Array.isArray(b.participants)?b.participants:[];const count=await replaceRoster(input);return send(res,200,{ok:true,count})}
  if(req.method==='POST'&&pathname==='/api/admin/periods'){const b=await bodyJson(req);try{const period=await createOrActivatePeriod(b.year,b.half,b.activate!==false);return send(res,200,{ok:true,period})}catch(e){return send(res,400,{error:e.message})}}
  if(req.method==='POST'&&pathname==='/api/admin/period/activate'){const b=await bodyJson(req);try{await activatePeriod(clean(b.periodId,60));return send(res,200,{ok:true,activePeriod:await getActivePeriod()})}catch(e){return send(res,400,{error:e.message})}}
  if(req.method==='POST'&&pathname==='/api/results'){const b=await bodyJson(req);const result=await saveResult(b);if(result.error)return send(res,result.status||400,{error:result.error});return send(res,200,result)}
  if(req.method==='GET'&&pathname==='/api/admin/results'){const data=await adminData();return send(res,200,{...data,storage:USE_POSTGRES?'PostgreSQL':'JSON (local test only)'})}
  return send(res,404,{error:'Not found'});
}

function serveStatic(req,res,pathname){let rel=pathname==='/'?'index.html':decodeURIComponent(pathname.replace(/^\//,''));rel=path.normalize(rel).replace(/^(\.\.[/\\])+/, '');const file=path.join(ROOT,rel);if(!file.startsWith(ROOT)||!fs.existsSync(file)||fs.statSync(file).isDirectory())return serveIndex(res);const buf=fs.readFileSync(file),ext=path.extname(file).toLowerCase();res.writeHead(200,{'Content-Type':MIME[ext]||'application/octet-stream','Content-Length':buf.length,'Cache-Control':ext==='.html'?'no-store':'public, max-age=3600'});res.end(buf)}
function serveIndex(res){const file=path.join(ROOT,'index.html');const buf=fs.readFileSync(file);res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Content-Length':buf.length,'Cache-Control':'no-store'});res.end(buf)}
const server=http.createServer(async(req,res)=>{const pathname=url.parse(req.url).pathname;try{if(pathname==='/health'){if(USE_POSTGRES)await pool.query('SELECT 1');return send(res,200,{ok:true,storage:USE_POSTGRES?'postgres':'json',activePeriod:await getActivePeriod()})}if(pathname.startsWith('/api/'))return await api(req,res,pathname);return serveStatic(req,res,pathname)}catch(e){console.error(e);return send(res,500,{error:'서버 오류가 발생했습니다.'})}});
initDb().then(async()=>{if(!USE_POSTGRES)await ensureLocalPeriod();server.listen(PORT,HOST,()=>{console.log(`NAVIMRO server running on http://${HOST}:${PORT}`);console.log(`Storage: ${USE_POSTGRES?'PostgreSQL':'JSON local fallback'}`);if(ADMIN_KEY==='change-me-before-deploy')console.warn('WARNING: ADMIN_KEY 환경변수를 반드시 변경하세요.');if(!USE_POSTGRES)console.warn('WARNING: DATABASE_URL이 없습니다. JSON 모드는 로컬 테스트용이며 Render 운영에는 PostgreSQL을 연결하세요.')})}).catch(err=>{console.error('Database initialization failed:',err);process.exit(1)});
process.on('SIGTERM',async()=>{if(pool)await pool.end().catch(()=>{});server.close(()=>process.exit(0))});
