const { query } = require('express');
let mongo_connection = require('./connection');

function capitalizeWords(word){
  word=word.toLowerCase();
  let finalSentence = word.replace(/(^\w{1})|(\s+\w{1})/g, letter => letter.toUpperCase())
  return finalSentence;
}

exports.obtener_trx = function (req, res, next, res_function) {
    var dbo=mongo_connection.dbo;
    dbo.collection("transaccion_log").find().toArray(function(err, result) {
        if (err) console.log(err);
        res_function(null,result, req, res, next);
    });
};

exports.obtener_trx_n_periodo = function(req, res, next, res_function){
  var dbo=mongo_connection.dbo;
  let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
  let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
  let provincia = req.query.provincia? req.query.provincia:'';
  let ciudad = req.query.ciudad? req.query.ciudad:'';
  let barrio = req.query.barrio? req.query.barrio:'';
  let producto = req.query.producto? req.query.producto:'';
  
  let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, 
                     "producto":new RegExp('^' + producto),
                     "provincia": new RegExp('^' + provincia.toUpperCase()), 
                     "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                     "barrio": new RegExp('^' + capitalizeWords(barrio))};
  dbo.collection("transaccion_log").find(querytrx).toArray(function(err2, resulttrx) {
        if (err2) console.log(err2);
        res_function(null,resulttrx, req, res, next);
      });
}


exports.obtener_promedio_trx_n_periodo = function(req, res, next, res_function){
  var dbo=mongo_connection.dbo;
  let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
  let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
  let provincia = req.query.provincia? req.query.provincia:'';
  let comercio = req.query.comercio? req.query.comercio:'';
  let ciudad = req.query.ciudad? req.query.ciudad:'';
  let barrio = req.query.barrio? req.query.barrio:'';
  let producto = req.query.producto? req.query.producto:'';
  
  let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, 
                     "producto":new RegExp('^' + producto),
                     "provincia": new RegExp('^' + provincia.toUpperCase()), 
                     "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                     "barrio": new RegExp('^' + capitalizeWords(barrio)),
                     "idcomercio": new RegExp('^' + comercio)};

  dbo.collection("transaccion_log").aggregate([{$match: querytrx}, 
                                                   {$group: {_id: "$fecha_trx", "prom_trx":{$avg: "$monto"}}}, 
                                                   {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {

        if (err2) console.log(err2);
        res_function(null,resulttrx, req, res, next);
      });
}

exports.obtener_promedio_trx = function(req, res, next, res_function){
    var dbo=mongo_connection.dbo;
    let provincia = req.query.provincia? req.query.provincia:'';
    let comercio = req.query.comercio? req.query.comercio:'';
    let ciudad = req.query.ciudad? req.query.ciudad:'';
    let barrio = req.query.barrio? req.query.barrio:'';
    let producto = req.query.producto? req.query.producto:'';
    let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                       "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                       "barrio": new RegExp('^' + capitalizeWords(barrio)),
                       "codigo_merchant": new RegExp('^' + comercio)};
    dbo.collection("comercio").distinct("codigo_merchant", querycomercio, 
      (function(err, result) {
        if (err) console.log(err);
        let querytrx= {"idcomercio": { $in: result}, "producto":new RegExp('^' + producto)};
        dbo.collection("transaccion_log").aggregate([{$match: querytrx}, 
                                                     {$group: {_id: "$fecha_trx", "prom_trx":{$avg: "$monto"}}}, 
                                                     {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {
  
          if (err2) console.log(err2);
          res_function(null,resulttrx, req, res, next);
        });
      }));
}


exports.obtener_n_trx_n_periodo = function(req, res, next, res_function){
  var dbo=mongo_connection.dbo;
  let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
  let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
  let provincia = req.query.provincia? req.query.provincia:'';
  let comercio = req.query.comercio? req.query.comercio:'';
  let ciudad = req.query.ciudad? req.query.ciudad:'';
  let barrio = req.query.barrio? req.query.barrio:'';
  let producto = req.query.producto? req.query.producto:'';

  let querytrx= {"fecha": { $lt : fecha_fin, $gte: fecha_inicio}, 
                     "producto":new RegExp('^' + producto),
                     "provincia": new RegExp('^' + provincia.toUpperCase()), 
                     "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                     "barrio": new RegExp('^' + capitalizeWords(barrio)),
                     "idcomercio": new RegExp('^' + comercio)};

  dbo.collection("transaccion_log_processed").aggregate([{$match: querytrx}, 
                                                   {$group: {_id: "$fecha", "n_trx":{$sum: "$num_trx"}}}, 
                                                   {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {

        if (err2) console.log(err2);
        res_function(null,resulttrx, req, res, next);
      });
}

exports.obtener_n_trx_n_periodos_before = function(req, res, next, res_function){
  var dbo=mongo_connection.dbo;

  let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
  let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
  let provincia = req.query.provincia? req.query.provincia:'';
  let comercio = req.query.comercio? req.query.comercio:'';
  let ciudad = req.query.ciudad? req.query.ciudad:'';
  let barrio = req.query.barrio? req.query.barrio:'';
  let producto = req.query.producto? req.query.producto:'';
  let periodo = req.query.periodo ? req.query.periodo : '';

  let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, 
                 "provincia": new RegExp('^' + provincia.toUpperCase()), 
                 "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                 "barrio": new RegExp('^' + capitalizeWords(barrio)),
                 "idcomercio": new RegExp('^' + comercio),
                 "producto": new RegExp('^' +producto)};
      
      
      if(periodo === "DIARIO")
      {
        dbo.collection("transaccion_log").aggregate([
          {
            $match: querytrx
          },
          {
            $group: {
              _id: {"fecha_trx":"$fecha_trx", "producto":"$producto"}, 
              "n_trx":{$sum: 1}
            }
          }, 
          {
            $sort: {_id: -1}}])
            .toArray(function(err2, resulttrx) {
  
          if (err2) console.log(err2);
          res_function(null,resulttrx, req, res, next);
        });
      }
      else if(periodo === "MENSUAL")
      {
        dbo.collection("transaccion_log").aggregate([
          {
            $match: querytrx
          },
          {
            $project : {
              fecha : {$substr: ['$fecha_trx', 0, 6]},
              n_trx: 1,
              producto: 1
  
            }
          },
          {
            $group: {
              _id: {"fecha_trx":"$fecha", "producto":"$producto"}, 
              "n_trx":{$sum: 1}
            }
          }, 
          {
            $sort: {_id: -1}}])
            .toArray(function(err2, resulttrx) {
  
          if (err2) console.log(err2);
          res_function(null,resulttrx, req, res, next);
        });
      }
      else if(periodo === "ANUAL")
      {
        dbo.collection("transaccion_log").aggregate([
          {
            $match: querytrx
          },
          {
            $project : {
              fecha : {$substr: ['$fecha_trx', 0, 4]},
              n_trx: 1,
              producto: 1
  
            }
          },
          {
            $group: {
              _id: {"fecha_trx":"$fecha", "producto":"$producto"},
              "n_trx":{$sum: 1}
            }
          }, 
          {
            $sort: {_id: -1}}])
            .toArray(function(err2, resulttrx) {
  
          if (err2) console.log(err2);
          res_function(null,resulttrx, req, res, next);
        });
      }
      
      
   
}


exports.obtener_n_trx_n_periodos = function(req, res, next, res_function){
  var dbo=mongo_connection.dbo;

  let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
  let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
  let provincia = req.query.provincia? req.query.provincia:'';
  let comercio = req.query.comercio? req.query.comercio:'';
  let ciudad = req.query.ciudad? req.query.ciudad:'';
  let barrio = req.query.barrio? req.query.barrio:'';
  let producto = req.query.producto? req.query.producto:'';
  let periodo = req.query.periodo ? req.query.periodo : '';

  let querytrx= {"fecha": { $lt : fecha_fin, $gte: fecha_inicio}, 
                 "provincia": new RegExp('^' + provincia.toUpperCase()), 
                 "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                 "barrio": new RegExp('^' + capitalizeWords(barrio)),
                 "idcomercio": new RegExp('^' + comercio),
                 "producto": new RegExp('^' +producto)};
      
    if(producto=="PAGO DE SERVICIOS"){
      querytrx= {"fecha": { $lt : fecha_fin, $gte: fecha_inicio}, 
                   "producto": {$in:["PAGO DE SERVICIOS", "RECAUDACIONES"]}, 
                   "provincia": new RegExp('^' + provincia.toUpperCase()), 
                   "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                   "idcomercio": new RegExp('^' + comercio),
                  "barrio": new RegExp('^' + capitalizeWords(barrio))};
    }
      if(periodo === "DIARIO")
      {
        dbo.collection("transaccion_log_processed").aggregate([
          {
            $match: querytrx
          },
          {
            $group: {
              _id: {"fecha_trx":"$fecha", "producto":"$producto"}, 
              "n_trx":{$sum: "$num_trx"}
            }
          },
          {
            $sort: {"n_trx": -1}
          }
        ])
            .toArray(function(err2, resulttrx) {
  
          if (err2) console.log(err2);
          res_function(null,resulttrx, req, res, next);
        });
      }
      else if(periodo === "MENSUAL")
      {
        dbo.collection("transaccion_log_processed").aggregate([
          {
            $match: querytrx
          },
          {
            $project : {
              fecha_trx : {$substr: ['$fecha', 0, 6]},
              n_trx: 1,
              producto: 1,
              num_trx:1
  
            }
          },
          {
            $group: {
              _id: {"fecha_trx":"$fecha_trx", "producto":"$producto"}, 
              "n_trx":{$sum: "$num_trx"}
            }
          },
          {
            $sort: {"n_trx": -1}
          }
        ])
            .toArray(function(err2, resulttrx) {
  
          if (err2) console.log(err2);
          res_function(null,resulttrx, req, res, next);
        });
      }
      else if(periodo === "ANUAL")
      {
        dbo.collection("transaccion_log_processed").aggregate([
          {
            $match: querytrx
          },
          {
            $project : {
              fecha_trx : {$substr: ['$fecha', 0, 4]},
              n_trx: 1,
              producto: 1,
              num_trx:1
  
            }
          },
          {
            $group: {
              _id: {"fecha_trx":"$fecha_trx", "producto":"$producto"},
              "n_trx":{$sum: "$num_trx"}
            }
          },
          {
            $sort: {"n_trx": -1}
          }
        ])
            .toArray(function(err2, resulttrx) {
  
          if (err2) console.log(err2);
          res_function(null,resulttrx, req, res, next);
        });
      }
      
      
   
}


exports.obtener_suma_trx_n_periodo = function(req, res, next, res_function){
  var dbo = mongo_connection.dbo;
  let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
  let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
  let provincia = req.query.provincia? req.query.provincia:'';
  let ciudad = req.query.ciudad? req.query.ciudad:'';
  let barrio = req.query.barrio? req.query.barrio:'';
  let producto = req.query.producto? req.query.producto:'';
  let periodo = req.query.periodo ? req.query.periodo : '';

  //let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, "idcomercio": { $in: result}, "producto":new RegExp('^' + producto)};
    let querytrx= {"fecha": { $lt : fecha_fin, $gte: fecha_inicio}, 
                   "producto": new RegExp('^' +producto), 
                   "provincia": new RegExp('^' + provincia.toUpperCase()), 
                   "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                  "barrio": new RegExp('^' + capitalizeWords(barrio))};
    
    if(producto=="PAGO DE SERVICIOS"){
      querytrx= {"fecha": { $lt : fecha_fin, $gte: fecha_inicio}, 
                   "producto": {$in:["PAGO DE SERVICIOS", "RECAUDACIONES"]}, 
                   "provincia": new RegExp('^' + provincia.toUpperCase()), 
                   "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                  "barrio": new RegExp('^' + capitalizeWords(barrio))};
    }
    
    if(periodo === "DIARIO")
    {
      dbo.collection("transaccion_log_processed").aggregate([
        {
          $match: querytrx,
        }, 
        {
          $group: {
            _id: {"fecha_trx":"$fecha", "producto":"$producto"} , 
            "prom_trx":{$sum: "$total"},
          }
        },
        {
          $sort: {"prom_trx": -1}
        }
      ])
      .toArray(function(err2, resulttrx) {
        if (err2) console.log(err2);
        res_function(null,resulttrx, req, res, next);
        });
    }
    else if(periodo === "MENSUAL")
    {
      
      dbo.collection("transaccion_log_processed").aggregate([
        {
          $match: querytrx,

        },
        {
          $project : {
            fecha_trx : {$substr: ['$fecha', 0, 6]},
            prom_trx: 1,
            producto : 1,
            total : 1

          }
        }, 
        {
          $group: {
            //_id: {$substr: ['$fecha_trx', 0, 6]}, 
            _id: {"fecha_trx":"$fecha_trx", "producto":"$producto"}, 
            "prom_trx":{$sum: "$total"},
            
          }
        },
        {
          $sort: {"prom_trx": -1}
        }
      ])
      .toArray(function(err2, resulttrx) {

        if (err2) console.log(err2);
        res_function(null,resulttrx, req, res, next);
        });
    }
    else if(periodo === "ANUAL")
    {
      dbo.collection("transaccion_log_processed").aggregate([
        {
          $match: querytrx,
        },
        {
          $project : {
            fecha_trx : {$substr: ['$fecha', 0, 4]},
            prom_trx: 1,
            producto : 1,
            total : 1
          }
        }, 
        {
          $group: {
            //_id: {$substr: ['$fecha_trx', 0, 4]}, 
            _id: {"fecha_trx":"$fecha_trx", "producto":"$producto"}, 
            "prom_trx":{$sum: "$total"},
            
          }
        },
        {
          $sort: {"prom_trx": -1}
        }
      ])
      .toArray(function(err2, resulttrx) {

        if (err2) console.log(err2);
        res_function(null,resulttrx, req, res, next);
        });
      
    }
  }

exports.obtener_suma_trx_n_periodo_before = function(req, res, next, res_function){
  var dbo = mongo_connection.dbo;
  let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
  let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
  let provincia = req.query.provincia? req.query.provincia:'';
  let ciudad = req.query.ciudad? req.query.ciudad:'';
  let barrio = req.query.barrio? req.query.barrio:'';
  let producto = req.query.producto? req.query.producto:'';
  let periodo = req.query.periodo ? req.query.periodo : '';

  //let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, "idcomercio": { $in: result}, "producto":new RegExp('^' + producto)};
    let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, 
                   "producto": new RegExp('^' +producto), 
                   "provincia": new RegExp('^' + provincia.toUpperCase()), 
                   "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                  "barrio": new RegExp('^' + capitalizeWords(barrio))};

    if(periodo === "DIARIO")
    {

      dbo.collection("transaccion_log").aggregate([
        {
          $match: querytrx,
        }, 
        {
          $group: {
            _id: {"fecha_trx":"$fecha_trx", "producto":"$producto"} , 
            "prom_trx":{$sum: "$monto"},
          }
        }, 
        {
          $sort: {_id: -1}
        }
      ])
      .toArray(function(err2, resulttrx) {

        if (err2) console.log(err2);
        res_function(null,resulttrx, req, res, next);
        });
    }
    else if(periodo === "MENSUAL")
    {
      
      dbo.collection("transaccion_log").aggregate([
        {
          $match: querytrx,

        },
        {
          $project : {
            fecha: {$substr: ['$fecha', 0, 6]},
            prom_trx: 1,
            producto : 1,
            monto : 1

          }
        }, 
        {
          $group: {
            //_id: {$substr: ['$fecha_trx', 0, 6]}, 
            _id: {"fecha_trx":"$fecha", "producto":"$producto"}, 
            "prom_trx":{$sum: "$total"},
            
          }
        }, 
        {
          $sort: {_id: -1}
        }
      ])
      .toArray(function(err2, resulttrx) {

        if (err2) console.log(err2);
        res_function(null,resulttrx, req, res, next);
        });
    }
    else if(periodo === "ANUAL")
    {
      dbo.collection("transaccion_log").aggregate([
        {
          $match: querytrx,
        },
        {
          $project : {
            fecha : {$substr: ['$fecha_trx', 0, 4]},
            prom_trx: 1,
            producto : 1,
            monto : 1
          }
        }, 
        {
          $group: {
            //_id: {$substr: ['$fecha_trx', 0, 4]}, 
            _id: {"fecha_trx":"$fecha", "producto":"$producto"}, 
            "prom_trx":{$sum: "$monto"},
            
          }
        }, 
        {
          $sort: {_id: -1}
        }
      ])
      .toArray(function(err2, resulttrx) {

        if (err2) console.log(err2);
        res_function(null,resulttrx, req, res, next);
        });
      
    }


}


// exports.obtener_promedio_trx_n_periodo = function(req, res, next, res_function){
//   var dbo=mongo_connection.dbo;
//   let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
//   let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
//   let provincia = req.query.provincia? req.query.provincia:'';
//   let comercio = req.query.comercio? req.query.comercio:'';
//   let ciudad = req.query.ciudad? req.query.ciudad:'';
//   let barrio = req.query.barrio? req.query.barrio:'';
//   let producto = req.query.producto? req.query.producto:'';
//   let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
//                      "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
//                      "barrio": new RegExp('^' + capitalizeWords(barrio)),
//                      "codigo_merchant": new RegExp('^' + comercio)};
//   dbo.collection("comercio").distinct("codigo_merchant", querycomercio, 
//     (function(err, result) {
//       console.log("result comereerr", result)
//       if (err) console.log(err);
//       let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, "idcomercio": { $in: result}, "producto":new RegExp('^' + producto)};
//       dbo.collection("transaccion_log").aggregate([{$match: querytrx}, 
//                                                    {$group: {_id: "$fecha_trx", "prom_trx":{$avg: "$monto"}}}, 
//                                                    {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {

//         if (err2) console.log(err2);
//         res_function(null,resulttrx, req, res, next);
//       });
//     }));
// }


  function calcular_promedio(ventas, attr){
    var suma=0;
    for(var i=0; i<ventas.length; i++){
      suma+=parseFloat(ventas[i][attr].toString());
    }
    return {"prom_por_comercios": (suma/ventas.length)}
  }

  exports.obtener_promedio_trx_n_periodo_por_comercio = function(req, res, next, res_function){
    var dbo=mongo_connection.dbo;
    let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
    let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
    let provincia = req.query.provincia? req.query.provincia:'';
    let comercio = req.query.comercio? req.query.comercio:'';
    let ciudad = req.query.ciudad? req.query.ciudad:'';
    let barrio = req.query.barrio? req.query.barrio:'';
    let producto = req.query.producto? req.query.producto:'';
    let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                       "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                       "barrio": new RegExp('^' + capitalizeWords(barrio)),
                       "codigo_merchant": new RegExp('^' + comercio)};
    dbo.collection("comercio").distinct("codigo_merchant", querycomercio, 
      (function(err, result) {
        console.log("result comereerr", result)
        if (err) console.log(err);
        let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, "idcomercio": { $in: result}, "producto":new RegExp('^' + producto)};
        dbo.collection("transaccion_log").aggregate([{$match: querytrx}, 
                                                     {$group: {_id: "$idcomercio", "prom_trx":{$avg: "$monto"}}}, 
                                                     {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {
  
          if (err2) console.log(err2);
          console.log("resultado por comercioo", resulttrx);
          var new_result=calcular_promedio(resulttrx, "prom_trx");
          res_function(null,new_result, req, res, next);
        });
      }));
    }


    exports.obtener_informacion_por_locacion = function(req, res, next, res_function){
      //no funciona
      var dbo=mongo_connection.dbo;
      let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
      let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
      let provincia = req.query.provincia? req.query.provincia:'';
      let comercio = req.query.comercio? req.query.comercio:'';
      let ciudad = req.query.ciudad? req.query.ciudad:'';
      let barrio = req.query.barrio? req.query.barrio:'';
      let producto = req.query.producto? req.query.producto:'';
      
      let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, "producto":new RegExp('^' + producto)};
      dbo.collection('transaccion_log').aggregate([
        {
          $lookup: {
            from: 'comercio',
            localField: 'idcomercio',
            foreignField: 'codigo_merchant',
            as: 'comercio'
          }
        },
        {
          $unwind: '$comercio'
        }
      ]).toArray(function(err2, resulttrx) {
              if (err2) console.log(err2);
              console.log("resultado por comercioo", resulttrx);
              res_function(null,resulttrx, req, res, next);  
            })
      }


exports.obtener_total_trx_n_periodo_x_lugar = function(req, res, next, res_function){
  var dbo=mongo_connection.dbo;
  let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
  let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
  let provincia = req.query.provincia? req.query.provincia:'';
  let ciudad = req.query.ciudad? req.query.ciudad:'';
  let barrio = req.query.barrio? req.query.barrio:'';

  console.log(barrio)

  let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                      "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                      "barrio": new RegExp('^' + capitalizeWords(barrio))};
  dbo.collection("comercio").distinct("codigo_merchant", querycomercio, 
    (function(err, result) {
      if (err) console.log(err);
      //let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, "idcomercio": { $in: result}, "producto":new RegExp('^' + producto)};
      let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, "idcomercio": { $in: result}};
      dbo.collection("transaccion_log").aggregate([
        {
          $match: querytrx
        }, 
        {
          $group: 
          {
            //_id: "$fecha_trx",
            _id: null,
             "n_trx":{$sum: 1}
          }
        }, 
        {
          $sort: 
          {
            _id: -1
          }
        }])
        .toArray(function(err2, resulttrx) {

        if (err2) console.log(err2);
        res_function(null,resulttrx, req, res, next);
      });
    }));
}

exports.obtener_total_monetario_trx_n_periodo_x_lugar = function(req, res, next, res_function){
  var dbo=mongo_connection.dbo;
  let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
  let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
  let provincia = req.query.provincia? req.query.provincia:'';
  let ciudad = req.query.ciudad? req.query.ciudad:'';
  let barrio = req.query.barrio? req.query.barrio:'';

  console.log(barrio)

  let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                      "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                      "barrio": new RegExp('^' + capitalizeWords(barrio))};
  //dbo.collection("comercio").distinct("codigo_merchant", querycomercio,
  dbo.collection("comercio").distinct("codigo_merchant",
    (function(err, result) {
      if (err) console.log(err);
      //let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, "idcomercio": { $in: result}, "producto":new RegExp('^' + producto)};
      let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, "idcomercio": { $in: result}};
      dbo.collection("transaccion_log").aggregate([
        {
          $match: querytrx
        }, 
        {
          $group: 
          {
            //_id: "$fecha_trx",
            //_id: null,
            _id: "$provincia",
            SUMA:{
              $sum: "$monto"
            }
            //  "n_trx":{$sum: 1}
          }
        }, 
        {
          $sort: 
          {
            _id: -1
          }
        }])
        .toArray(function(err2, resulttrx) {

        if (err2) console.log(err2);
        res_function(null,resulttrx, req, res, next);
      });
    }));
}

exports.obtener_suma_trx_n_periodo_x_lugar_before = function(req, res, next, res_function){
  var dbo=mongo_connection.dbo;
  let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
  let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
  let granularidad = req.query.granularidad? req.query.granularidad: '' ;
  let producto = req.query.producto? req.query.producto: '' ;

  let querytrx;

  if(producto !== "TODOS")
  {
    querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, "producto":new RegExp('^' + producto)};
  }
  else
  {
    querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}};
  }

  

  if(granularidad === "PROVINCIA")
  {
    dbo.collection("transaccion_log").aggregate([
      {
        $match: querytrx
      },
      {
        $group: 
        {
          _id: "$provincia",
          SUMA:{
            $sum: "$monto"
          },
          "n_trx":{$sum: 1}
          //  "n_trx":{$sum: 1}
        }
      }, 
      {
        $sort: 
        {
          _id: -1
        }
      }
  
    ]).toArray(function(err2, resulttrx) {
  
      if (err2) console.log(err2);
      res_function(null,resulttrx, req, res, next);
    });
  }
  else if(granularidad === "CIUDAD")
  {
    dbo.collection("transaccion_log").aggregate([
      {
        $match: querytrx
      },
      {
        $group: 
        {
          _id: "$ciudad",
          SUMA:{
            $sum: "$monto"
          },
          "n_trx":{$sum: 1}
          //  "n_trx":{$sum: 1}
        }
      }, 
      {
        $sort: 
        {
          _id: -1
        }
      }
  
    ]).toArray(function(err2, resulttrx) {
  
      if (err2) console.log(err2);
      res_function(null,resulttrx, req, res, next);
    });
  }
  else if(granularidad === "BARRIO")
  {
    dbo.collection("transaccion_log").aggregate([
      {
        $match: querytrx
      },
      {
        $group: 
        {
          _id: "$barrio",
          SUMA:{
            $sum: "$monto"
          },
          "n_trx":{$sum: 1}
          //  "n_trx":{$sum: 1}
        }
      }, 
      {
        $sort: 
        {
          _id: -1
        }
      }
  
    ]).toArray(function(err2, resulttrx) {
  
      if (err2) console.log(err2);
      res_function(null,resulttrx, req, res, next);
    });
  }
}


exports.obtener_suma_trx_n_periodo_x_lugar = function(req, res, next, res_function){
  var dbo=mongo_connection.dbo;
  let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
  let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
  let granularidad = req.query.granularidad? req.query.granularidad: '' ;
  let producto = req.query.producto? req.query.producto: '' ;

  let querytrx;

  if(producto !== "TODOS")
  {
    querytrx= {"fecha": { $lt : fecha_fin, $gte: fecha_inicio}, "producto":new RegExp('^' + producto)};
    if(producto=="PAGO DE SERVICIOS"){
      querytrx= {"fecha": { $lt : fecha_fin, $gte: fecha_inicio}, "producto":{$in: ["PAGO DE SERVICIOS", "RECAUDACIONES"]}};
    }
  }
  else
  {
    querytrx= {"fecha": { $lt : fecha_fin, $gte: fecha_inicio}};
  }

  

  if(granularidad === "PROVINCIA")
  {
    dbo.collection("transaccion_log_processed").aggregate([
      {
        $match: querytrx
      },
      {
        $group: 
        {
          _id: "$provincia",
          SUMA:{
            $sum: "$total"
          },
          "n_trx":{$sum: "$num_trx"}
        }
      }
    ]).toArray(function(err2, resulttrx) {
  
      if (err2) console.log(err2);
      res_function(null,resulttrx, req, res, next);
    });
  }
  else if(granularidad === "CIUDAD")
  {
    dbo.collection("transaccion_log_processed").aggregate([
      {
        $match: querytrx
      },
      {
        $group: 
        {
          _id: "$ciudad",
          SUMA:{
            $sum: "$total"
          },
          "n_trx":{$sum: "$num_trx"}
        }
      }  
    ]).toArray(function(err2, resulttrx) {
  
      if (err2) console.log(err2);
      res_function(null,resulttrx, req, res, next);
    });
  }
  else if(granularidad === "BARRIO")
  {
    dbo.collection("transaccion_log_processed").aggregate([
      {
        $match: querytrx
      },
      {
        $group: 
        {
          _id: "$barrio",
          SUMA:{
            $sum: "$total"
          },
          "n_trx":{$sum: "$num_trx"}
        }
      }
    ]).toArray(function(err2, resulttrx) {
  
      if (err2) console.log(err2);
      res_function(null,resulttrx, req, res, next);
    });
  }
}

exports.obtener_cantidad_trx_x_periodo_por_locacion = function(req, res, next, res_function){
  var dbo=mongo_connection.dbo;
  let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
  let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
  let granularidad = req.query.granularidad? req.query.granularidad: '' ;


  let querytrx= {"fecha": { $lt : fecha_fin, $gte: fecha_inicio}};


  if(granularidad === "PROVINCIA")
  {
    dbo.collection("transaccion_log_processed").aggregate([
      {
        $match: querytrx
      },
      {
        $group: 
        {
          _id: "$provincia",
          "n_trx":{$sum: "$num_trx"}
          //  "n_trx":{$sum: 1}
        }
      }, 
      {
        $sort: 
        {
          _id: -1
        }
      }
  
    ]).toArray(function(err2, resulttrx) {
  
      if (err2) console.log(err2);
      res_function(null,resulttrx, req, res, next);
    });
  }
  else if(granularidad === "CIUDAD")
  {
    dbo.collection("transaccion_log_processed").aggregate([
      {
        $match: querytrx
      },
      {
        $group: 
        {
          _id: "$ciudad",
          "n_trx":{$sum: "$num_trx"}
          //  "n_trx":{$sum: 1}
        }
      }, 
      {
        $sort: 
        {
          _id: -1
        }
      }
  
    ]).toArray(function(err2, resulttrx) {
  
      if (err2) console.log(err2);
      res_function(null,resulttrx, req, res, next);
    });
  }
  else if(granularidad === "BARRIO")
  {
    dbo.collection("transaccion_log_processed").aggregate([
      {
        $match: querytrx
      },
      {
        $group: 
        {
          _id: "$barrio",
          "n_trx":{$sum: "$num_trx"}
          //  "n_trx":{$sum: 1}
        }
      }, 
      {
        $sort: 
        {
          _id: -1
        }
      }
  
    ]).toArray(function(err2, resulttrx) {
  
      if (err2) console.log(err2);
      res_function(null,resulttrx, req, res, next);
    });
  }
}

exports.obtener_cantidad_trx_x_periodo_por_locacion_before = function(req, res, next, res_function){
  var dbo=mongo_connection.dbo;
  let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
  let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
  let granularidad = req.query.granularidad? req.query.granularidad: '' ;


  let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}};


  if(granularidad === "PROVINCIA")
  {
    dbo.collection("transaccion_log").aggregate([
      {
        $match: querytrx
      },
      {
        $group: 
        {
          _id: "$provincia",
          "n_trx":{$sum: 1}
          //  "n_trx":{$sum: 1}
        }
      }, 
      {
        $sort: 
        {
          _id: -1
        }
      }
  
    ]).toArray(function(err2, resulttrx) {
  
      if (err2) console.log(err2);
      res_function(null,resulttrx, req, res, next);
    });
  }
  else if(granularidad === "CIUDAD")
  {
    dbo.collection("transaccion_log").aggregate([
      {
        $match: querytrx
      },
      {
        $group: 
        {
          _id: "$ciudad",
          "n_trx":{$sum: 1}
          //  "n_trx":{$sum: 1}
        }
      }, 
      {
        $sort: 
        {
          _id: -1
        }
      }
  
    ]).toArray(function(err2, resulttrx) {
  
      if (err2) console.log(err2);
      res_function(null,resulttrx, req, res, next);
    });
  }
  else if(granularidad === "BARRIO")
  {
    dbo.collection("transaccion_log").aggregate([
      {
        $match: querytrx
      },
      {
        $group: 
        {
          _id: "$barrio",
          "n_trx":{$sum: 1}
          //  "n_trx":{$sum: 1}
        }
      }, 
      {
        $sort: 
        {
          _id: -1
        }
      }
  
    ]).toArray(function(err2, resulttrx) {
  
      if (err2) console.log(err2);
      res_function(null,resulttrx, req, res, next);
    });
  }
}

exports.obtener_valor_total_maximo = function(req, res, next, res_function){
      var dbo=mongo_connection.dbo;
      let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
      let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
      let localidad1 = req.query.localidad1? req.query.localidad1:'default';
      let localidad2 = req.query.localidad2? req.query.localidad2:'default';
      let producto = req.query.producto? req.query.producto:'';
      let descripcion = req.query.descripcion? req.query.descripcion:'provincia';
      let periodo = req.query.periodo? req.query.periodo:'';
      
      let querytrx= {"fecha": { $lt : fecha_fin, $gte: fecha_inicio}, 
                     "producto":new RegExp('^' + producto)}
      if(localidad1=="default" && localidad2 != "default"){
            querytrx[descripcion]= new RegExp('^' + localidad2);
      }
      else{
        if(localidad2=="default" && localidad1 != "default"){
          querytrx[descripcion]= new RegExp('^' + localidad1);
        }
        else{
          if(localidad2!="default" && localidad1 != "default"){
            querytrx[descripcion]= {$in: [localidad1, localidad2]};
          }
        }
      }
    console.log("query", querytrx)
    if(periodo === "DIARIO")
    {

      dbo.collection("transaccion_log_processed").aggregate([
        {
          $match: querytrx,
        }, 
        {
          $group: {
            _id: {"fecha_trx":"$fecha"} , 
            "prom_trx":{$sum: "$total"},
          }
        }, 
        {
          $sort: {"prom_trx": -1}
        }
      ])
      .toArray(function(err2, resulttrx) {

        if (err2) console.log(err2);
        console.log("result", resulttrx)
        if(resulttrx!=[])
          res_function(null,[resulttrx[0]], req, res, next);
        else
          res_function(null,resulttrx, req, res, next);
        });
    }
    else if(periodo === "MENSUAL")
    {
      
      dbo.collection("transaccion_log_processed").aggregate([
        {
          $match: querytrx,

        },
        {
          $group: {
            _id:  {$substr: ['$fecha', 0, 6]}, 
            "prom_trx":{$sum: "$total"},
            
          }
        }, 
        {
          $sort: {"prom_trx": -1}
        }
      ])
      .toArray(function(err2, resulttrx) {
        console.log("result", resulttrx)
        if (err2) console.log(err2);
        if(resulttrx!=[])
          res_function(null,[resulttrx[0]], req, res, next);
        else
          res_function(null,resulttrx, req, res, next);
        });

    }
    else if(periodo === "ANUAL")
    {
      dbo.collection("transaccion_log_processed").aggregate([
        {
          $match: querytrx
        },
        {
          $group: {
            _id: {$substr: ['$fecha', 0, 4]},
            "prom_trx":{$sum: "$total"},
          }
        },
        {
          $sort: {"prom_trx": -1}}])
          .toArray(function(err2, resulttrx) {
            console.log("result", resulttrx)
        if (err2) console.log(err2);
        if(resulttrx!=[])
          res_function(null,[resulttrx[0]], req, res, next);
        else
          res_function(null,resulttrx, req, res, next);
      });
    }
  }



  exports.obtener_cantidad_maxima = function(req, res, next, res_function){
    var dbo=mongo_connection.dbo;
    let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
    let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
    let localidad1 = req.query.localidad1? req.query.localidad1:'default';
    let localidad2 = req.query.localidad2? req.query.localidad2:'default';
    let producto = req.query.producto? req.query.producto:'';
    let descripcion = req.query.descripcion? req.query.descripcion:'provincia';
    let periodo = req.query.periodo? req.query.periodo:'';
    
    let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, 
                   "producto":new RegExp('^' + producto)}
    if(localidad1=="default" && localidad2 != "default"){
          querytrx[descripcion]= new RegExp('^' + localidad2);
    }
    else{
      if(localidad2=="default" && localidad1 != "default"){
        querytrx[descripcion]= new RegExp('^' + localidad1);
      }
      else{
        if(localidad2!="default" && localidad1 != "default"){
          querytrx[descripcion]= {$in: [localidad1, localidad2]};
        }
      }
    }
  console.log("query", querytrx)
  if(periodo === "DIARIO")
  {

    dbo.collection("transaccion_log").aggregate([
      {
        $match: querytrx,
      }, 
      {
        $group: {
          _id: {"fecha_trx":"$fecha_trx"} , 
          "prom_trx":{$sum: 1},
        }
      }, 
      {
        $sort: {"prom_trx": -1}
      }
    ])
    .toArray(function(err2, resulttrx) {

      if (err2) console.log(err2);
      console.log("result", resulttrx)
      if(resulttrx!=[])
        res_function(null,[resulttrx[0]], req, res, next);
      else
        res_function(null,resulttrx, req, res, next);
      });
  }
  else if(periodo === "MENSUAL")
  {
    
    dbo.collection("transaccion_log").aggregate([
      {
        $match: querytrx,

      },
      {
        $group: {
          _id:  {$substr: ['$fecha_trx', 0, 6]}, 
          "prom_trx":{$sum: 1},
          
        }
      }, 
      {
        $sort: {"prom_trx": -1}
      }
    ])
    .toArray(function(err2, resulttrx) {
      console.log("result", resulttrx)
      if (err2) console.log(err2);
      if(resulttrx!=[])
        res_function(null,[resulttrx[0]], req, res, next);
      else
        res_function(null,resulttrx, req, res, next);
      });

  }
  else if(periodo === "ANUAL")
  {
    dbo.collection("transaccion_log").aggregate([
      {
        $match: querytrx
      },
      {
        $group: {
          _id: {$substr: ['$fecha_trx', 0, 4]},
          "prom_trx":{$sum: 1},
        }
      },
      {
        $sort: {"prom_trx": -1}}])
        .toArray(function(err2, resulttrx) {
          console.log("result", resulttrx)
      if (err2) console.log(err2);
      if(resulttrx!=[])
        res_function(null,[resulttrx[0]], req, res, next);
      else
        res_function(null,resulttrx, req, res, next);
    });
  }
}


exports.obtener_suma_trx_por_comercio = function(req, res, next, res_function){
  var dbo = mongo_connection.dbo;
  let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
  let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
  let provincia = req.query.provincia? req.query.provincia:'';
  let ciudad = req.query.ciudad? req.query.ciudad:'';
  let barrio = req.query.barrio? req.query.barrio:'';
  let producto = req.query.producto? req.query.producto:'';


  //let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, "idcomercio": { $in: result}, "producto":new RegExp('^' + producto)};
    let querytrx= {"fecha": { $lt : fecha_fin, $gte: fecha_inicio}, 
                   "provincia": new RegExp('^' + provincia.toUpperCase()), 
                   "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                  "barrio": new RegExp('^' + capitalizeWords(barrio))};
  dbo.collection("transaccion_log_processed").aggregate([
                    {
                      $match: querytrx
                    },
                    {
                      $group: {
                        _id:{"comercio":'$idcomercio',
                            "producto":'$producto'},
                        "suma":{$sum: '$total'},
                      }
                    }])
                      .toArray(function(err2, resulttrx) {
                        console.log("result", resulttrx)
                    if (err2) console.log(err2);
                    res_function(null,resulttrx, req, res, next);
                  });
                }