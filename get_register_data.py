import urllib.request
import pprint
import json

host_ = '18080'
pass_ = 'tatuya1300'

def generate_token():
    obj = {'APIPassword': pass_ }
    json_data = json.dumps(obj).encode('utf8')
    url = f'http://localhost:{host_}/kabusapi/token'
    req = urllib.request.Request(url, json_data, method='POST')
    req.add_header('Content-Type', 'application/json')
    try:
        with urllib.request.urlopen(req) as res:
            content = json.loads(res.read())
            token_value = content.get('Token')
    except urllib.error.HTTPError as e:
        print(e)
    return token_value


def unregister_all(token):
    url = 'http://localhost:18080/kabusapi/unregister/all'
    req = urllib.request.Request(url, method='PUT')
    req.add_header('Content-Type', 'application/json')
    req.add_header('X-API-KEY', token)

    try:
        with urllib.request.urlopen(req) as res:
            content = json.loads(res.read())
            return content
    except urllib.error.HTTPError as e:
        print(f"HTTPError: {e}")
        content = json.loads(e.read())
        return content
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def register_symbols(token, symbols):
    obj = {'Symbols': symbols}
    json_data = json.dumps(obj).encode('utf8')
    url = 'http://localhost:18080/kabusapi/register'
    req = urllib.request.Request(url, json_data, method='PUT')
    req.add_header('Content-Type', 'application/json')
    req.add_header('X-API-KEY', token)

    try:
        with urllib.request.urlopen(req) as res:
            content = json.loads(res.read())
            return content
    except urllib.error.HTTPError as e:
        print(f"HTTPError: {e}")
        content = json.loads(e.read())
        return content
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

symbols = [
        {'Symbol': '6537', 'Exchange': 1},
        {'Symbol': '3791', 'Exchange': 1},
        {'Symbol': '7049', 'Exchange': 1},
        {'Symbol': '6537', 'Exchange': 1},
        {'Symbol': '6557', 'Exchange': 1},
        {'Symbol': '4582', 'Exchange': 1},
        {'Symbol': '5610', 'Exchange': 1},
        {'Symbol': '6937', 'Exchange': 1},
        {'Symbol': '4935', 'Exchange': 1},
    ]


token_value = generate_token()
pprint.pprint(unregister_all(token_value))
pprint.pprint(register_symbols(token_value,symbols))
