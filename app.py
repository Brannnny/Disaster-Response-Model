import datetime
from flask import Flask, render_template
from pymongo import MongoClient
from common.utils import InitDBConnection as conn

# TODO: how to use create_app function to avoid creating multiple mongodb connection


class Test:
    def __init__(self, fname, lname, mname):
        self.fname = fname
        self.lname = lname
        self.mname = mname


def create_app():
    app = Flask(__name__)

    client = MongoClient(conn.mongodb_connection())
    print([e for e in client.ODS.entries.find({})])
    client.ODS.entries.insert_one({'fname': 'Bukola', 'mname': 'Omolewa', 'lname': 'Adaramola'})
    app.db = client.ODS

    @app.route("/")
    def hello_world():
        return render_template("firstpage.html")

    @app.route("/page")
    def page():
        return render_template("secondpage.html")

    @app.route("/pythonobj")
    def obj():
        testing = Test("Omolewa", "Adaramola", "Bukola")
        return render_template("thirdpage.html", testing=testing)

    return app


def main():
    app = create_app()
    app.run(host='localhost', port=5000, debug=True)


if __name__ == '__main__':
    main()