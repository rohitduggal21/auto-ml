from flask import Flask, render_template, request, Response, jsonify, session, g
from werkzeug.utils import secure_filename
import os
from con_util import *

UPLOAD_FOLDER = '/home/rohit/Desktop/UI/uploads'
ALLOWED_EXTENSIONS = {'csv'}
app = Flask(__name__)
app.config['SECRET_KEY'] = 'your_secret_key'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'db'): g.db = give_connection(session['credentials'])
    return g.db

@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'db'):
        g.db.close()

@app.route('/')
def index():	
	session.clear()
	return render_template('index.html')

@app.route('/get_attr',methods=['POST'])
def get_attr():	
	key = request.get_json()['key']
	try:	
		con = get_db()
		results = con.get(request.get_json())
	except Exception as error:	
		print(str(error))
		return jsonify(status=False,msg='Try Again!')
	return jsonify(**{'status':True,key:results})

@app.route('/upload',methods=['POST'])
def upload():	
	if 'file' not in request.files:	
		return jsonify(status=False,msg='No File Chosen!')
	elif allowed_file(request.files['file'].filename):	
		filename = secure_filename(request.files['file'].filename)
		request.files['file'].save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
		if 'credentials' not in session.keys(): session['credentials'] = {'source':'csv'}
		return jsonify(status=True,msg=filename)
	else:	
		return jsonify(status=False,msg=request.files['file'].filename)

@app.route('/connect',methods=['POST'])
def connect():	
	try:
		session['credentials'] = request.get_json()
		con = get_db()
	except Exception as error:	
		print(str(error))
		return jsonify(status=False)
	return jsonify(status=True,schemas=con.get_schemas())

if __name__ == '__main__': 
	app.run(debug=True,threaded=True)
