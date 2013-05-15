var startTime = new Date().getTime();

require("ep_etherpad-lite/node_modules/npm").load({}, function(er,npm) {

  var fs = require("fs");

  var redis = require('redis');

  var settings = require("ep_etherpad-lite/node/utils/Settings");
  var log4js = require('ep_etherpad-lite/node_modules/log4js');

  var dbWrapperSettings = {
    cache: 0,
    writeInterval: 100,
    json: false // data is already json encoded
  };

  var sqlFile = process.argv[2];

  //stop if the settings file is not set
  if(!sqlFile)
  {
    console.error("Use: node importLargeSqlFile.js $SQLFILE");
    process.exit(1);
  }

  process.stdout.write("Start importing keys...\n");

  var keyNo = 0;
  var buffer = '';
  var fp = fs.createReadStream(sqlFile, {encoding: 'utf8'});
  var matchRegex = RegExp('^REPLACE INTO store VALUES', 'i');
  var redisClient = redis.createClient();

  fp.on('data', function(data) {
    var lines = data.split(/\r?\n/),
        keyValues = [];

    lines[0] = buffer + data[0];
    buffer = lines.pop();

    lines.forEach(function(l) {
      if (matchRegex.test(l) === true) {
        var pos = l.indexOf("', '");
        var key = l.substr(28, pos - 28);
        var value = l.substr(pos + 3);
        value = value.substr(0, value.length - 2);
        keyValues = keyValues.concat([key, unescape(value)]);
      }
    });

    if (keyValues) {
      redisClient.mset(keyValues, function(err) {
        if (err) {
          console.log(err);
          return;
        }
        console.log('Saving ' + keyValues.length / 2 + ' keys to redis.');
      })
    }
  });


  fp.on('end', function() {
    process.stdout.write("\n");
    process.stdout.write("done. waiting for db to finish transaction. depended on dbms this may take some time...\n");
    process.exit(0);
  });
});

function log(str)
{
  console.log((new Date().getTime() - startTime)/1000 + "\t" + str);
}

unescape = function(val) {
  // value is a string
  if (val.substr(0, 1) == "'") {
    val = val.substr(0, val.length - 1).substr(1);

    return val.replace(/\\[0nrbtZ\\'"]/g, function(s) {
      switch(s) {
        case "\\0": return "\0";
        case "\\n": return "\n";
        case "\\r": return "\r";
        case "\\b": return "\b";
        case "\\t": return "\t";
        case "\\Z": return "\x1a";
        default: return s.substr(1);
      }
    });
  }

  // value is a boolean or NULL
  if (val == 'NULL') {
    return null;
  }
  if (val == 'true') {
    return true;
  }
  if (val == 'false') {
    return false;
  }

  // value is a number
  return val;
};
