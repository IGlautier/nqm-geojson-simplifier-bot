module.exports = (function() {  
  var topojson = require("topojson");
  var fs = require("fs");
  var path = require("path");
  var _ = require("lodash");

  function propertyTransform(feature) {
    var properties = {
      OBJECTID: feature.properties.objectid,
      LSOA11CD: feature.properties.lsoa11cd,
      LSOA11NM: feature.properties.lsoa11nm,
      LSOA11NMW: feature.properties.lsoa11nmw,
      area: feature.properties.st_areashape
    };
    return properties;
  }

  function databot(input, output, context) {
    output.progress(0);
    const config = {
      commandHost: context.commandHost,
      queryHost: context.queryHost,
      accessToken: context.authToken
    };
    const api = context.tdxApi;
    output.debug("fetching data for %s", input.geojsonFileId);
    api.getDatasetData(input.geojsonFileId, null, null, {limit:50000}, (err, response) => {
      if(err) {
        output.error("Failed to get data - %s", err.message);
        process.exit(1);
      }
      else {
        output.debug("got data");
        output.progress(50);
      
        const filename = path.resolve("./simplified.json");
        output.debug("writing file %s", filename);
        var collection = {
          type: "FeatureCollection", 
          features: response.data
        };
        var topology = topojson.topology({collection: collection}, {
          "property-transform": propertyTransform,
          "quantization": 1e4
        });

        topology = topojson.simplify(topology, {"coordinate-system":"spherical", "minimum-area": 1e-8});

        var geojson = topojson.feature(topology, topology.objects.collection);
      
        fs.writeFile(filename, JSON.stringify(geojson), (err) => {
          if(err) {
            output.error("Failed to write file - %s", err.message);
            process.exit(1);
          } else {          
            output.progress(100);
            process.exit(0);
          }
        });

      }
    });
  }

  var input = require("nqm-databot-utils").input;
  input.pipe(databot);
}());


