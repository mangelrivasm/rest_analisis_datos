var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/";
var namedb="web_platform_db"

async function listDatabases(client){
  var databasesList = await client.db().admin().listDatabases();
  console.log("Databases:");
  databasesList.databases.forEach(db => console.log(` - ${db.name}`));
};

async function listCollections(dbo){
  var collectionsList = await dbo.collections();
  console.log("Collections:");
  console.log(collectionsList);
};

MongoClient.connect(url, function(err, db) {
  if (err) throw err;
  const dbo = db.db(namedb);
  listCollections(dbo);
  console.log("model db", dbo);
  exports.dbo=dbo;
});


