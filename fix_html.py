import re

def fix():
    with open('mexc_scalperV5.py', 'r', encoding='utf-8') as f:
        src = f.read()
    
    match = re.search(r'def index\(\):\s*return Response\(\"\"\"(.*?)\"\"\"', src, re.DOTALL)
    if not match:
        print("Could not find HTML in source")
        return
    
    html = match.group(1)
    print("Found HTML, len:", len(html))
    
    with open('mexc_scalperV5.1.py', 'r', encoding='utf-8') as f:
        dst = f.read()
        
    dst = re.sub(r'def index\(\):\s*return Response\(\"\"\"(.*?)\"\"\"', 'def index():\n            return Response(\"\"\"' + html.replace('\\', '\\\\') + '\"\"\"', dst, flags=re.DOTALL)
    
    with open('mexc_scalperV5.1.py', 'w', encoding='utf-8') as f:
        f.write(dst)
    
    print("Fixed mexc_scalperV5.1.py")

if __name__ == "__main__":
    fix()
