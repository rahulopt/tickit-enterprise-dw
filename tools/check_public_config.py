#!/usr/bin/env python3
"""Check publishable Git files for credentials and deployment-specific identifiers.

Use --staged to inspect the exact index blobs that will enter the next commit.
Findings print locations and categories, never the matched secret value.
"""
import argparse
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PATTERNS = {
    'AWS access key': re.compile(r'\b(?:AKIA|ASIA)[A-Z0-9]{16}\b'),
    'private key': re.compile(r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----'),
    'GitHub token': re.compile(r'\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b'),
    'account-specific AWS ARN': re.compile(r'arn:aws[a-z-]*:[^:\s]+:[^:\s]*:(\d{12}):[^\s\"\']+'),
    'account-specific bucket name': re.compile(r'\b[a-z0-9][a-z0-9.-]*-\d{12}\b'),
    'local home path': re.compile(r'/(?:Users|home)/[A-Za-z0-9_.-]+/'),
}
PRIVATE_PATH = re.compile(
    r'(^|/)(\.aws|deployment-backups|\.venv)(/|$)'
    r'|(^|/)\.env($|\.(?!example$))'
    r'|(^|/)config\.local\.json$|\.rendered\.(json|sql)$'
    r'|(^|/)credentials$|\.(pem|key)$'
)
SECRET_ASSIGNMENT = re.compile(
    r'^\s*(?:AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN|GITHUB_TOKEN|GH_TOKEN)'
    r'\s*=\s*[\"\']?([^\s\"\']+)', re.M
)


def violations(path, data):
    if PRIVATE_PATH.search(path):
        return [(1, 'private/generated file')]
    try:
        content = data.decode('utf-8')
    except UnicodeDecodeError:
        return []
    findings = []
    for label, pattern in PATTERNS.items():
        for match in pattern.finditer(content):
            # AWS's fictitious example account is allowed in test fixtures only.
            if label == 'account-specific AWS ARN' and path.startswith('tests/') and match[1] == '123456789012':
                continue
            findings.append((content.count('\n', 0, match.start()) + 1, label))
    for match in SECRET_ASSIGNMENT.finditer(content):
        if not match[1].startswith('REPLACE_WITH_'):
            findings.append((content.count('\n', 0, match.start()) + 1, 'credential assignment'))
    return findings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--staged', action='store_true')
    args = parser.parse_args()
    command = ['git', 'ls-files', '-z', '--cached']
    if not args.staged:
        command += ['--others', '--exclude-standard']
    names = subprocess.check_output(command, cwd=ROOT).decode().split('\0')
    total = 0
    failed = False
    for name in sorted(set(filter(None, names))):
        if args.staged:
            data = subprocess.check_output(['git', 'show', ':' + name], cwd=ROOT)
        else:
            path = ROOT / name
            if not path.is_file():
                continue
            data = path.read_bytes()
        total += 1
        for line, category in violations(name, data):
            print(f'{name}:{line}: {category}')
            failed = True
    print(f'Checked {total} publishable files: {"FAIL" if failed else "PASS"}')
    raise SystemExit(1 if failed else 0)


if __name__ == '__main__':
    main()
