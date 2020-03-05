#
from http.server import HTTPServer, BaseHTTPRequestHandler
import pickle
import pandas as pd


'''
# Prereq, stash model using the following

from joblib import dump, load
dump(clf, 'filename.joblib')

OR

c = pickle.dumps(clf)
'''

# load model
# clf = load('filename.joblib')
# if packaged with pickle
# clf = pickle.loads()


# configure server
class S(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

    def do_GET(self):
        self._set_headers()
        self.send_response(200, 'OK')
        self.end_headers()

    def do_HEAD(self):
        self._set_headers()

    def do_POST(self):
        if 'content-length' in self.headers:
            # length = int(self.headers.get('content-length'))
            length = int(self.headers['Content-Length'])
            print("length: " + str(length))
            field_data = self.rfile.read(length).decode("utf-8")
            # load request as json
            # expecting request body like so
            # {"speed": [30, 40, 50]}
            res = json.loads(field_data)
            # convert json to dataframe
            df = pd.read_json(res)
            # prediction_results = clf.predict(df)
            prediction_results = df.to_dict # setting to dummy file for now
            # clf.score
            self.wfile.write(json.dumps(prediction_results))
            self.send_response(200, "OK")
            self.end_headers()
        else:
            self.send_response(204, "Invalid request, no data received")
            self.end_headers()

def run(server_class=HTTPServer, handler_class=S, addr="0.0.0.0", port=8000):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print("starting server on port " + str(port))
    httpd.serve_forever()

run()
