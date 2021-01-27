const { query } = require('express');
let mongo_connection = require('./connection');

function capitalizeWords(word){
  word=word.toLowerCase();
  let finalSentence = word.replace(/(^\w{1})|(\s+\w{1})/g, letter => letter.toUpperCase())
  return finalSentence;
}

exports.obtener_ejecutivos = function (req, res, next, res_function) {
    var dbo=mongo_connection.dbo;
    dbo.collection("visitas_ejecutivos").distinct("nombres", 
        (function(err, result) {
          if (err) console.log(err);
          res_function(null,result, req, res, next);
          })
    )
};

exports.obtener_visitas_ejecutivos = function (req, res, next, res_function) {
    var dbo=mongo_connection.dbo;
    let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
    let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
    dbo.collection("visitas_ejecutivos").find().toArray((function(err, result) {
          if (err) console.log(err);
          res_function(null,result, req, res, next);
          })
    )
};



