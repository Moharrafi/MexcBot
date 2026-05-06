import re
import sys

def fix_html():
    # 1. Read the original HTML from mexc_scalperV5.py
    with open('mexc_scalperV5.py', 'r', encoding='utf-8') as f:
        v5_content = f.read()
    
    match = re.search(r'(DASHBOARD_HTML\s*=\s*\"\"\"(?:.*?)\"\"\")', v5_content, re.DOTALL)
    if not match:
        print("Could not find DASHBOARD_HTML in mexc_scalperV5.py")
        sys.exit(1)
        
    dashboard_html_block = match.group(1)
    print(f"Found DASHBOARD_HTML block, size: {len(dashboard_html_block)}")

    # 2. Update mexc_scalperV5.1.py
    with open('mexc_scalperV5.1.py', 'r', encoding='utf-8') as f:
        v51_content = f.read()

    # In v5.1, it's returning a hardcoded string directly inside index()
    # def index(): return Response("""<!DOCTYPE html>...<!-- HTML tetap sama -->...""", mimetype="text/html")
    # Let's replace that response with DASHBOARD_HTML and inject DASHBOARD_HTML globally.

    # Find the Flask import or similar place to inject DASHBOARD_HTML if it's missing
    if "DASHBOARD_HTML = " not in v51_content:
        # Inject right before the ScalperBotV5 class
        v51_content = v51_content.replace('class ScalperBotV5:', f'{dashboard_html_block}\n\nclass ScalperBotV5:')
    
    # Now fix the route
    route_pattern = r'(@app\.route\(\"\/\"\)\n\s+def index\(\):\n?\s*return Response\()\"\"\"(?:.*?)\"\"\"'
    v51_content = re.sub(route_pattern, r'\1DASHBOARD_HTML', v51_content, flags=re.DOTALL)

    with open('mexc_scalperV5.1.py', 'w', encoding='utf-8') as f:
        f.write(v51_content)
    print("Fixed mexc_scalperV5.1.py successfully.")

if __name__ == "__main__":
    fix_html()
