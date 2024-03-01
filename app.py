from flask import Flask, send_from_directory, request, send_file
import nbformat
import os

app = Flask(__name__)

@app.route("/", methods=['GET', 'POST'])
def upload_file():
    for file in os.listdir('./output'):
        if file.endswith('.py'):
            os.remove(f'./output/{file}')
    if request.method == "POST" and request.files.get('file'):
        file = request.files["file"]
        filename = file.filename
        nb = nbformat.read(file, as_version=4)
        output = ""
        new_filename = f'./output/{filename.removesuffix(".ipynb")}.py'
        with open(new_filename, 'w') as f:
            for cell in nb.cells:
                if cell['cell_type'] == 'code':
                    output += cell['source']
                    f.write(cell['source'])
                output += '\n'
                f.write('\n')
        return send_file(new_filename, as_attachment=True)
    else:
        return send_from_directory('./', "index.html")