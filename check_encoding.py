import sys

# Check for surrogate issues in the HTML template
with open('/home/ubuntu/mexc_v3_bot/mexc_botV3.py', 'r', errors='replace') as f:
    content = f.read()

# Find the DASHBOARD_HTML section
idx = content.find('DASHBOARD_HTML')
if idx > 0:
    # Check for surrogate char issues
    html_start = content.find("'''", idx)
    if html_start > 0:
        html_end = content.find("'''", html_start + 3)
        html = content[html_start:html_end+3]
        try:
            html.encode('utf-8')
            print("HTML template encodes OK")
        except UnicodeEncodeError as e:
            print(f"ENCODING ERROR: {e}")
            # Find the problem area
            for i, c in enumerate(html):
                try:
                    c.encode('utf-8')
                except UnicodeEncodeError:
                    start = max(0, i-20)
                    end = min(len(html), i+20)
                    print(f"Problem char at position {i}: {repr(html[start:end])}")
                    print(f"Char: {repr(c)} ordinal: {ord(c)}")
else:
    print("DASHBOARD_HTML not found")

# Also check the full file
try:
    content.encode('utf-8')
    print("Full file encodes OK")
except UnicodeEncodeError as e:
    print(f"Full file encoding error: {e}")
