from importlib.resources import files as _files

def streamlit_app_path():
    return _files(__name__) / "streamlit_app.py"