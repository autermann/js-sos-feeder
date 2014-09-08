var log4js = require("log4js");
log4js.loadAppender("file");
log4js.addAppender(log4js.appenders.file("feeder.log"));
var logger = log4js.getLogger();

var fs = require("fs");
var async = require("async");
var xmlbuilder = require("xmlbuilder");

var request = require("request").defaults({
  url: 'http://localhost:8080/service',
  json: true,
  method: "POST",
  encoding: "utf8"
});


var SECOND = 1000;
var MINUTE = SECOND * 60;
var HOUR = MINUTE * 60;

var NUM_OBSERVABLE_PROPERTIES = 9;
var NUM_SENSORS = 140;
var NUM_FEATURES_PER_PROCEDURE = 1;
var NUM_OBSERVATIONS = 1 * NUM_SENSORS * NUM_OBSERVABLE_PROPERTIES * NUM_FEATURES_PER_PROCEDURE;
var OFFSET = 0;// 207560;


var procedureID = function(id) {
  return "http://www.52north.org/test/procedure/" + id;
};
var offeringID = function(id) {
  return "http://www.52north.org/test/offering/" + id;
};
var featureID = function(id) {
  return "http://www.52north.org/test/feature/" + id;
};
var observablePropertyID = function(id) {
  return "http://www.52north.org/test/observableProperty/" + id;
};

var featureOfInterest = function(id) {
  return {
    identifier: featureID(id),
    name: "feature" + id,
    sampledFeature: "http://www.opengis.net/def/nil/OGC/0/unknown",
    geometry: {
      type: "Point",
      coordinates: [ 50 + id , 7 + id/10 ],
    }
  };
};

var series = (function() {
  var i, j, k, array = [];

  for (i = 0; i < NUM_SENSORS; ++i) {
    for (j = 0; j < NUM_FEATURES_PER_PROCEDURE; ++j) {
      for (k = 0; k < NUM_OBSERVABLE_PROPERTIES; ++k) {
        var s = {
          procedure: procedureID(i),
          offering: offeringID(i),
          feature: featureOfInterest((j+1)*i),
          observedProperty: observablePropertyID(k),
          unit: "unit" + k,
        };
        array.push(s);
      }
    }
  }
  return array;
})();

var observableProperties = (function(){
  var j, array = [];
  for (j = 0; j < NUM_OBSERVABLE_PROPERTIES; ++j) {
    array.push(observablePropertyID(j));
  }
  return array;
}());


var sensorDescription = function(id) {
  var i;
  var root = xmlbuilder.create("sml:SensorML");
  root.att("xmlns:swe","http://www.opengis.net/swe/1.0.1")
    .att("xmlns:sml","http://www.opengis.net/sensorML/1.0.1")
    .att("xmlns:gml","http://www.opengis.net/gml")
    .att("version", "1.0.1");

  var system = root.ele("sml:member").ele("sml:System");

  system.ele("sml:identification")
    .ele("sml:IdentifierList")
      .ele("sml:identifier")
        .att("name", "uniqueID")
        .ele("sml:Term")
          .att("definition", "urn:ogc:def:identifier:OGC:1.0:uniqueID")
          .ele("sml:value")
            .txt(procedureID(id));
  /*
  system.ele("sml:capabilities")
    .att("name", "featureOfInterest")
    .ele("swe:SimpleDataRecord")
      .ele("swe:field")
        .att("name", "featureOfInterestID")
        .ele("swe:Text")
          .ele("swe:value")
            .txt(featureID(id));
  */

  system.ele("sml:capabilities")
    .att("name", "offerings")
    .ele("swe:SimpleDataRecord")
      .ele("swe:field")
        .att("name", "offering" + id)
        .ele("swe:Text")
          .att("definition", "urn:ogc:def:identifier:OGC:offeringID")
          .ele("swe:value")
            .txt(offeringID(id));

  system.ele("sml:position")
    .att("name", "sensorPosition")
    .ele("swe:Position")
      .att("referenceFrame", "urn:ogc:def:crs:EPSG::4326")
      .ele("swe:location")
        .ele("swe:Vector")
        .att("gml:id", "STATION_LOCATION")
        .ele("swe:coordinate")
          .att("name", "easting")
          .ele("swe:Quantity")
            .att("axisID", "x")
            .ele("swe:uom")
              .att("code", "degree")
            .up()
            .ele("swe:value")
              .txt("7.651968812254194")
            .up()
          .up()
        .up()
        .ele("swe:coordinate")
          .att("name", "northing")
          .ele("swe:Quantity")
            .att("axisID", "y")
            .ele("swe:uom")
              .att("code", "degree")
            .up()
            .ele("swe:value")
              .txt("51.935101100104916")
            .up()
          .up()
        .up()
        .ele("swe:coordinate")
          .att("name", "altitude")
          .ele("swe:Quantity")
            .att("axisID", "z")
            .ele("swe:uom")
              .att("code", "m")
            .up()
            .ele("swe:value")
              .txt("52.0");

  var inputList = system.ele("sml:inputs").ele("sml:InputList");
  var outputList = system.ele("sml:outputs").ele("sml:OutputList");

  for (i = 0; i < NUM_OBSERVABLE_PROPERTIES; ++i) {
    inputList.ele("sml:input")
      .att("name", "observableProperty" + i)
      .ele("swe:ObservableProperty")
        .att("definition", observablePropertyID(i));
    outputList.ele("sml:output")
      .att("name", "observableProperty" + i)
      .ele("swe:Quantity")
        .att("definition", observablePropertyID(i))
        .ele("swe:uom")
          .att("code", "NOT_DEFINED");
  }

  return root.toString({pretty:false});
};

var stringify = function(json) {
  return JSON.stringify(json, null, 2);
};

var responseHandler = function(errorCallback, callback) {
  return function(err, res, body) {
    if (!err && res.statusCode == 200) {
      callback(body);
    } else {
      errorCallback(err);
    }
  };
};

var getCapabilities = function(sections, errorCallback, callback) {
  if (!callback) {
    callback = sections;
    sections = null;
  }
  var payload = {
    service: "SOS",
    version: "2.0.0",
    request: "GetCapabilities"
  };
  if (sections) {
    payload.sections = sections;
  }
  request({ body: payload }, responseHandler(errorCallback, callback));
};


var insertSensors = function(callback) {
  logger.info("Starting sensor insertion");

  var index = 0;

  function test() {
    return index < NUM_SENSORS;
  }

  function fn(callback) {
    logger.info("Inserting Sensor " + index);
    var payload = {
      "request": "InsertSensor",
      "service": "SOS",
      "version": "2.0.0",
      "procedureDescriptionFormat": "http://www.opengis.net/sensorML/1.0.1",
      "procedureDescription": sensorDescription(index),
      "observableProperty": observableProperties,
      "observationType": [
        "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
      ],
      "featureOfInterestType": "http://www.opengis.net/def/samplingFeatureType/OGC-OM/2.0/SF_SamplingPoint"
    };
    request({body: payload}, function(err, res, body) {
      if (err) {
        callback(err);
      } else if (res.statusCode >= 400) {
        callback(body);
      } else {
        logger.info("Inserted sensor " + index);
        callback();
      }
    });
    ++index;
  }

  function finish(err) {
    if (err) {
      logger.error("Error: " + stringify(err));
    } else {
      logger.info("Finished sensor insertion");
      if (callback instanceof Function) {
        callback();
      }
    }
  }

  async.whilst(test, fn, finish);
};

var insertObservations = function(callback) {
  logger.info("Starting observation insertion");
  var index = OFFSET ? OFFSET : 0;
  var seriesIndex = 0;

  function test() {
    return index < NUM_OBSERVATIONS+OFFSET;
  }

 function fn(callback) {
    seriesIndex = (++seriesIndex % series.length);
    var s = series[seriesIndex];
    var obsTime = new Date(++index * 1000).toISOString().replace(".000Z", "Z");
    var payload = {
      request: "InsertObservation",
      service: "SOS",
      version: "2.0.0",
      offering: s.offering,
      observation: {
        type: "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        identifier: {
          value: "o_" + index
        },
        procedure: s.procedure,
        observedProperty: s.observedProperty,
        featureOfInterest: s.feature,
        phenomenonTime: obsTime,
        resultTime: obsTime,
        result: {
          uom: s.unit,
          value: Math.random()
        }
      }
    };
    var startTime = new Date().getTime();
    request({body: payload}, function(err, res, body) {
      if (err) {
        callback(err);
      } else if (res.statusCode >= 400) {
        callback(body);
      } else {
        logger.info("Inserted observation " + index + " in " + (new Date().getTime()-startTime) + "ms");
        callback();
      }
    });
  }

  function finish(err) {
    if (err) {
      logger.error("Error: " + stringify(err));
    } else {
      logger.info("Finished observation insertion");
      if (callback instanceof Function) {
        callback();
      }
    }
  }

  async.whilst(test, fn, finish);
};



var insertObservationsSingleRequest = function(callback) {
  logger.info("Starting observation insertion");
  var index = OFFSET ? OFFSET : 0;
  var seriesIndex = 0;
  var s = series[seriesIndex];
  var observations = [];

  function test() {
    return index < NUM_OBSERVATIONS+OFFSET;
  }

  while (index < NUM_OBSERVATIONS+OFFSET) {
    //seriesIndex = (++seriesIndex % series.length);

    var obsTime = new Date(++index * 1000).toISOString().replace(".000Z", "Z");
    observations.push({
      type: "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
      identifier: {
        value: "o_" + index
      },
      procedure: s.procedure,
      observedProperty: s.observedProperty,
      featureOfInterest: s.feature,
      phenomenonTime: obsTime,
      resultTime: obsTime,
      result: {
        uom: s.unit,
        value: Math.random()
      }
    });
  }

  var payload = {
    request: "InsertObservation",
    service: "SOS",
    version: "2.0.0",
    offering: s.offering,
    observation: observations
  };
  var startTime = new Date().getTime();
  request({body: payload}, function(err, res, body) {
    if (err) {
      callback(err);
    } else if (res.statusCode >= 400) {
      callback(body);
    } else {
      logger.info("Inserted "+observations.length+" observation in " + (new Date().getTime()-startTime) + "ms");
      callback();
    }
  });
};


//insertObservationsSingleRequest(function(err){
//  if (err) logger.error(stringify(err));
//});

//insertSensors(function() {
  insertObservations(function() {
  });
//});