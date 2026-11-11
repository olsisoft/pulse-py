#!/usr/bin/env python3
"""Extract hash-verified public SDK fixtures from a canonical Pulse product JAR.

No checkout or source-directory fallback is accepted. The supplied subject must
be the actual qualified PE1 candidate, not the historical baseline by convention.
"""
import argparse,hashlib,json,pathlib,re,tempfile,zipfile,shutil
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--product-jar',type=pathlib.Path,required=True);p.add_argument('--sha256',required=True);p.add_argument('--source-head',required=True);p.add_argument('--source-tree',required=True);p.add_argument('--output',type=pathlib.Path,required=True);a=p.parse_args()
if not re.fullmatch(r'[0-9a-f]{64}',a.sha256):p.error('Expected explicit product JAR SHA256')
if not all(re.fullmatch(r'[0-9a-f]{40}',v) for v in [a.source_head,a.source_tree]):p.error('Expected explicit qualified candidate HEAD/tree')
jar=a.product_jar.resolve();out=a.output.resolve();raw=jar.read_bytes()
if hashlib.sha256(raw).hexdigest()!=a.sha256:raise SystemExit('Canonical product JAR digest mismatch')
if out.exists() and any(out.iterdir()):raise SystemExit('Output must be fresh/empty; stale fixtures are never reused')
out.parent.mkdir(parents=True,exist_ok=True)
with tempfile.TemporaryDirectory(prefix='pulse-public-fixtures-',dir=out.parent) as td:
 stage=pathlib.Path(td);records=[]
 with zipfile.ZipFile(jar) as z:
  names=[v.filename for v in z.infolist() if (v.filename.startswith('developer-hub/') and v.filename.endswith('.md')) or v.filename=='openapi/openapi.yaml']
  if len(names)!=len(set(names)) or 'openapi/openapi.yaml' not in names or not any(v.startswith('developer-hub/') for v in names):raise SystemExit('Incomplete/duplicate canonical public resources')
  for name in sorted(names):
   part=pathlib.PurePosixPath(name)
   if part.is_absolute() or '..' in part.parts:raise SystemExit('Unsafe JAR resource path')
   data=z.read(name);dst=stage/name;dst.parent.mkdir(parents=True,exist_ok=True);dst.write_bytes(data);records.append({'path':name,'sha256':hashlib.sha256(data).hexdigest()})
 manifest={'schema':1,'source_repository':'olsisoft/pulse','source_head':a.source_head,'source_tree':a.source_tree,'product_jar_sha256':a.sha256,'resources':records,'scope':'Public OpenAPI/developer-hub fixtures from actual packaged product; no source path or historical reactor'}
 (stage/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
 if out.exists():out.rmdir()
 shutil.copytree(stage,out)
print(json.dumps({'result':'PASS','resources':len(records),'source_head':a.source_head,'source_tree':a.source_tree,'product_jar_sha256':a.sha256,'output':str(out)}))
