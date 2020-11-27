from flask import Flask, url_for, jsonify
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import pandas as pd
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.functions import min

app = Flask(__name__)


def has_no_empty_params(rule):
    defaults = rule.defaults if rule.defaults is not None else ()
    arguments = rule.arguments if rule.arguments is not None else ()
    return len(defaults) >= len(arguments)


@app.route("/api/v1.0/precipitation")
def precipitation():
    session = Session()
    result = session.query(Measurement.date, Measurement.prcp). \
        filter(Measurement.date >= '2016-08-24', Measurement.date <= '2017-08-23'). \
        order_by(Measurement.date).all()
    result = pd.DataFrame(data=result)
    result.set_index('date', inplace=True)
    return jsonify(result.prcp.to_dict())


@app.route("/api/v1.0/stations")
def stations():
    session = Session()
    result = []
    # Get all the stations
    stations = session.query(Station).all()
    for station in stations:
        # Convert the result to a dictionary
        result.append({
            "id": station.id,
            "station": station.station,
            "name": station.name,
            "latitude": station.latitude,
            "longitude": station.longitude,
            "elevation": station.elevation
        })
    return jsonify(result)


@app.route("/api/v1.0/tobs")
def tobs():
    session = Session()
    most_active = session.query(Measurement.station,func.count(Measurement.station)).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).first()
    result = session.query(Measurement.date, Measurement.tobs).\
        filter(Measurement.date >= '2016-08-24', Measurement.date <= '2017-08-23', Measurement.station == most_active[0]).\
        order_by(Measurement.date).all()
    tobs = []
    for tob in result:
        tobs.append({
            "date": tob[0],
            "tobs": tob[1]
        })
    return jsonify(tobs)


@app.route("/api/v1.0/<start>", defaults={'end': None})
@app.route("/api/v1.0/<start>/<end>")
def temperatures_summaries(start, end):
    session = Session()
    if end is None:
        # Get the maximum date in the db
        max_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
        end = max_date[0]
    sel = [func.min(Measurement.tobs),
           func.max(Measurement.tobs),
           func.avg(Measurement.tobs)]

    result = session.query(*sel).filter(Measurement.date > start, Measurement.date <= end).first()
    return jsonify({
        "MinTemperature": result[0],
        "MaxTemperature": result[1],
        "AvgTemperature": result[2]
    })


@app.route("/")
def home():
    links = []
    for rule in app.url_map.iter_rules():
        if "GET" in rule.methods and has_no_empty_params(rule):
            url = url_for(rule.endpoint, **(rule.defaults or {}))
            links.append(url)
    return jsonify(links)


if __name__ == '__main__':
    # Perform a warm start
    engine = create_engine("sqlite:///hawaii.sqlite")
    Base = automap_base()
    Base.prepare(engine, reflect=True)
    # Create objets for all the database tables
    Measurement = Base.classes.measurement
    Station = Base.classes.station
    # Create session to query
    session_factory = sessionmaker(bind=engine)
    Session = scoped_session(session_factory)
    app.run()