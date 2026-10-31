#!/usr/bin/env python3
"""Package previously downloaded Linux wheels/installed layers; never runs pip/AWS."""
import hashlib
import json
import zipfile
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
BUILD=ROOT/'tests/isolated/build'


def build(directory=BUILD):
    directory=Path(directory)
    sizes={}
    for layer in ('sdk','data'):
        source=directory/(layer+'-layer')
        files=sorted(p for p in source.rglob('*') if p.is_file() and not {'__pycache__','tests','include'}.intersection(p.relative_to(source).parts) and p.suffix!='.pyc' and 'bin' not in p.relative_to(source).parts)
        if not files:raise ValueError('Install target-platform wheels before packaging '+layer)
        for path in files:
            if path.suffix=='.so' and path.read_bytes()[:4]!=b'\x7fELF':
                raise ValueError('Non-Linux native binary: '+str(path))
        size=sum(p.stat().st_size for p in files);sizes[layer]=size
        with zipfile.ZipFile(directory/(layer+'-layer.zip'),'w',zipfile.ZIP_DEFLATED) as archive:
            for path in files:
                name=str(path.relative_to(source))
                info=zipfile.ZipInfo(name,(2026,1,1,0,0,0));info.compress_type=zipfile.ZIP_DEFLATED
                archive.writestr(info,path.read_bytes())
    if sizes['data']+sizes['sdk']+1024*1024>250*1024*1024:
        raise ValueError('Lambda uncompressed function plus layers exceeds 250 MiB')
    wheels=sorted((directory/'wheels').glob('*.whl'))
    if not wheels or any('macosx' in p.name for p in wheels):raise ValueError('Use Linux or pure Python wheels only')
    pins={}
    for name in ('data','sdk'):
        for line in (ROOT/'packaging'/(name+'.requirements.txt')).read_text().splitlines():
            if line: pins[line.split('==')[0]]=line.split('==')[1]
    if len(wheels)!=len(pins):raise ValueError('Wheel count differs from the exact pinned dependency closure')
    with zipfile.ZipFile(directory/'glue-dependencies.gluewheels.zip','w',zipfile.ZIP_STORED) as archive:
        archive.writestr('wheels/requirements.txt','\n'.join(k+'=='+v for k,v in sorted(pins.items()))+'\n')
        for wheel in wheels:archive.write(wheel,'wheels/'+wheel.name)
    sums={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in wheels}
    (directory/'wheel-lock.json').write_text(json.dumps(sums,indent=2)+'\n')
    (directory/'layer-sizes.json').write_text(json.dumps(sizes,indent=2)+'\n')
    return sizes


if __name__=='__main__':print(json.dumps(build(),indent=2))
