from flask import Flask, request

# REDIRECT APP
app = Flask(__name__)

@app.route("/auth")
def auth():
    code = request.args.get("code")
    state = request.args.get("state")
    return f"Authorization Code: {code}, State: {state}"

if __name__ == "__main__":
    app.run(port=8000)
