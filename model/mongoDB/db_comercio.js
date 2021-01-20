const { query } = require('express');
let mongo_connection = require('./connection');

function capitalizeWords(word){
  word=word.toLowerCase();
  let finalSentence = word.replace(/(^\w{1})|(\s+\w{1})/g, letter => letter.toUpperCase())
  return finalSentence;
}

exports.obtener_comercios = function (req, res, next, res_function) {
    var dbo=mongo_connection.dbo;
    dbo.collection("comercio").find().toArray(function(err, result) {
        if (err) console.log(err);
        res_function(null,result, req, res, next);
      });
};

exports.obtener_comercios_por_locacion = function(req, res, next, res_function){
    var dbo=mongo_connection.dbo;
    let provincia = req.query.provincia? req.query.provincia:'';
    let comercio = req.query.comercio? req.query.comercio:'';
    let ciudad = req.query.ciudad? req.query.ciudad:'';
    let barrio = req.query.barrio? req.query.barrio:'';
    let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                       "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                       "barrio": new RegExp('^' + capitalizeWords(barrio)),
                       "id": parseInt(comercio)};
    dbo.collection("comercio").find(querycomercio).toArray(function(err2, resulttrx) {
          if (err2) console.log(err2);
          console.log("comerciooos", resulttrx);
          res_function(null,resulttrx, req, res, next);
        });
      };
    
  exports.obtener_comercios_por_fecha_creacion = function(req, res, next, res_function){
        var dbo=mongo_connection.dbo;
        let fecha_creacion = req.query.fecha_creacion? req.query.fecha_creacion: '';
        let provincia = req.query.provincia? req.query.provincia:'';
        let comercio = req.query.comercio? req.query.comercio:'';
        let ciudad = req.query.ciudad? req.query.ciudad:'';
        let barrio = req.query.barrio? req.query.barrio:'';
        let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                           "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                           "barrio": new RegExp('^' + capitalizeWords(barrio)),
                           "codigo_merchant": new RegExp('^' + comercio),
                           "fMrContrSignDate": fecha_creacion};
        dbo.collection("comercio").find(querycomercio).toArray(function(err2, resulttrx) {
              if (err2) console.log(err2);
              console.log("comerciooos", resulttrx);
              res_function(null,resulttrx, req, res, next);
            });
          };
      
    
    exports.obtener_rfm = function(req, res, next, res_function){
      var dbo=mongo_connection.dbo;
      let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
      let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
      let provincia = req.query.provincia? req.query.provincia:'';
      let comercio = req.query.comercio? req.query.comercio:'';
      let ciudad = req.query.ciudad? req.query.ciudad:'';
      let barrio = req.query.barrio? req.query.barrio:'';
      let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                         "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                         "barrio": new RegExp('^' + capitalizeWords(barrio)),
                         "codigo_merchant": new RegExp('^' + comercio)};
      dbo.collection("comercio").distinct("id", querycomercio, 
        (function(err, result) {
          if (err) console.log(err);
          console.log("RESULTTTT", result)
          let querytrx= {"fecha_deposito": { $lt : fecha_fin, $gte: fecha_inicio}, "id_comercio": { $in: result}};
          dbo.collection("pagocomercio").aggregate([{$match: querytrx}, 
                                                    {$group: {_id: "$id_comercio", 
                                                              "ultimo_deposito":{$max: "$fecha_deposito"},
                                                              "numero_deposito":{$sum: 1},
                                                              "avg_deposito":{$avg: "$valor"}}}, 
                                                    {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {
    
            if (err2) console.log(err2);
            console.log("RESULTADOOO FINAL", resulttrx);
            res_function(null,resulttrx, req, res, next);
          });
        }));
    }

    function getRFMAverage(rfm_comercios){
      var total_avg_deposito = 0;
      var total_days = 0;
      var total_numerodeposito = 0;
      for(var i=0; i<rfm_comercios.length; i++){
        total_avg_deposito+=parseFloat(rfm_comercios[i].avg_deposito.toString());
        total_days+=rfm_comercios[i].days;
        total_numerodeposito+=rfm_comercios[i].numero_deposito;
      }
      for(var j=0; j<rfm_comercios.length; j++){
        rfm_comercios[j]["percent_avg_deposito"]=(rfm_comercios[j].avg_deposito/total_avg_deposito)*100;
        rfm_comercios[j]["percent_numero_deposito"]=(rfm_comercios[j].numero_deposito/total_numerodeposito)*100;
        rfm_comercios[j]["percent_days"]=(rfm_comercios[j].days/total_days)*100;     
      }

      for(var k=0; k<rfm_comercios.length; k++){
        rfm_comercios[k]["monetary"]=getNumber(rfm_comercios[k].percent_avg_deposito);
        rfm_comercios[k]["frequency"]=getNumber(rfm_comercios[k].percent_numero_deposito);
        rfm_comercios[k]["recency"]=getNumberRecency(rfm_comercios[k].percent_days);  
      }

    }


    function getRFM(rfm_comercios){
      var numerodecomercios=rfm_comercios.length;
      rfm_comercios.sort(function(a, b) {
        return b.avg_deposito-a.avg_deposito;
      }); 
      
      var porcentaje=100/numerodecomercios;
      var counter=-porcentaje;
      for (var i=0; i<rfm_comercios.length; i++) {
        counter+=porcentaje;
        if(counter<20)
          rfm_comercios[i]["monetary"]=5;
        else{
          if(counter<40)
            rfm_comercios[i]["monetary"]=4;
          else{
            if(counter<60)
              rfm_comercios[i]["monetary"]=3;
            else{
              if(counter<80)
                rfm_comercios[i]["monetary"]=2;
               else{
                rfm_comercios[i]["monetary"]=1;
               }
            }
          }
        }
      }


      rfm_comercios.sort(function(a, b) {
        return b.numero_deposito-a.numero_deposito;
      }); 
      
      counter=-porcentaje;
      for (i=0; i<rfm_comercios.length; i++) {
        counter+=porcentaje;
        if(counter<20)
          rfm_comercios[i]["frequency"]=5;
        else{
          if(counter<40)
            rfm_comercios[i]["frequency"]=4;
          else{
            if(counter<60)
              rfm_comercios[i]["frequency"]=3;
            else{
              if(counter<80)
                rfm_comercios[i]["frequency"]=2;
               else{
                rfm_comercios[i]["frequency"]=1;
               }
            }
          }
        }
      }

      rfm_comercios.sort(function(a, b) {
        return b.days-a.days;
      }); 
      
      counter=-porcentaje;
      for (i=0; i<rfm_comercios.length; i++) {
        counter+=porcentaje;
        if(counter<20)
          rfm_comercios[i]["recency"]=1;
        else{
          if(counter<40)
            rfm_comercios[i]["recency"]=2;
          else{
            if(counter<60)
              rfm_comercios[i]["recency"]=3;
            else{
              if(counter<80)
                rfm_comercios[i]["recency"]=4;
               else{
                rfm_comercios[i]["recency"]=5;
               }
            }
          }
        }
      }
      return rfm_comercios;
    }



    function getDays(rfm_comercios, fechafin){
      var new_rfm_comercios=[];
      var pattern = /(\d{4})(\d{2})(\d{2})/;
      for(var i=0; i<rfm_comercios.length; i++){
        var date = new Date(rfm_comercios[i].ultimo_deposito.replace(pattern,'$1-$2-$3'));
        var datefin = new Date(fechafin.replace(pattern,'$1-$2-$3'));
        var time_difference = datefin.getTime() - date.getTime();
        var days_difference = time_difference / (1000 * 60 * 60 * 24); 
        rfm_comercios[i]["days"]=days_difference;
        new_rfm_comercios.push(rfm_comercios[i]);
      }
      return new_rfm_comercios;
    }


    function agregar_grupo_rfm_separado(rfm_comercios){
      for(var i=0; i<rfm_comercios.length; i++){
        var r=rfm_comercios[i].recency;
        var f=rfm_comercios[i].frequency;
        var m=rfm_comercios[i].monetary;
        if ((r==4 || r==5) && (f==4 || f==5) && (m==4 || m==5))
            rfm_comercios[i]["grupo_rfm"]="CAMPEON";
        if ((r>=2 && r<=5) && (f>=3 && f<=5) && (m>=3 && m<=5))
            rfm_comercios[i]["grupo_rfm"]="LEAL";   
        if ((r>=3 && r<=5) && (f>=1&& f<=3) && (m>=1 && m<=3))
            rfm_comercios[i]["grupo_rfm"]="LEAL EN POTENCIA";  
        if ((r>=4 && r<=5) && (f>=0 && f<=1) && (m>=0 && m<=1))
            rfm_comercios[i]["grupo_rfm"]="NUEVO";   
        if ((r>=3 && r<=4) && (f>=0 && f<=1) && (m>=0 && m<=1))
            rfm_comercios[i]["grupo_rfm"]="PROMETEDOR"; 
        if ((r>=2 && r<=3) && (f>=2 && f<=3) && (m>=2 && m<=3))
            rfm_comercios[i]["grupo_rfm"]="NECESITA ATENCION";   
        if ((r>=2 && r<=3) && (f>=0 && f<=2) && (m>=0 && m<=2))
            rfm_comercios[i]["grupo_rfm"]="POR DORMIRSE";  
        if ((r>=0 && r<=2) && (f>=2 && f<=5) && (m>=2 && m<=5))
            rfm_comercios[i]["grupo_rfm"]="EN RIESGO";   
        if ((r>=0 && r<=1) && (f>=4 && f<=5) && (m>=4 && m<=5))
            rfm_comercios[i]["grupo_rfm"]="NO PODEMOS PERDERLO";   
        if ((r>=1 && r<=2) && (f>=1 && f<=2) && (m>=1 && m<=2))
            rfm_comercios[i]["grupo_rfm"]="DORMIDO";  
        if ((r>=0 && r<=2) && (f>=0 && f<=2) && (m>=0 && m<=2))
            rfm_comercios[i]["grupo_rfm"]="PERDIDO";
      }
      return rfm_comercios;
    }

    function agregar_grupo_rfm(rfm_comercios){
      for(var i=0; i<rfm_comercios.length; i++){
        var r=rfm_comercios[i].recency;
        var f=rfm_comercios[i].frequency;
        var m=rfm_comercios[i].monetary;
        var fm=Math.trunc((f+m)/2)
        if ((r==4 || r==5) && (fm==4 || fm==5))
            rfm_comercios[i]["grupo_rfm"]="CAMPEON";
        if ((r>=2 && r<=5) && (fm>=3 && fm<=5))
            rfm_comercios[i]["grupo_rfm"]="LEAL";   
        if ((r>=3 && r<=5) && (fm>=1&& fm<=3))
            rfm_comercios[i]["grupo_rfm"]="LEAL EN POTENCIA";  
        if ((r>=4 && r<=5) && (fm>=0 && fm<=1))
            rfm_comercios[i]["grupo_rfm"]="NUEVO";   
        if ((r>=3 && r<=4) && (fm>=0 && fm<=1))
            rfm_comercios[i]["grupo_rfm"]="PROMETEDOR"; 
        if ((r>=2 && r<=3) && (fm>=2 && fm<=3))
            rfm_comercios[i]["grupo_rfm"]="NECESITA ATENCION";   
        if ((r>=2 && r<=3) && (fm>=0 && fm<=2))
            rfm_comercios[i]["grupo_rfm"]="POR DORMIRSE";  
        if ((r>=0 && r<=2) && (fm>=2 && fm<=5))
            rfm_comercios[i]["grupo_rfm"]="EN RIESGO";   
        if ((r>=0 && r<=1) && (fm>=4 && fm<=5))
            rfm_comercios[i]["grupo_rfm"]="NO PODEMOS PERDERLO";   
        if ((r>=1 && r<=2) && (fm>=1 && fm<=2))
            rfm_comercios[i]["grupo_rfm"]="DORMIDO";  
        if ((r>=0 && r<=2) && (fm>=0 && fm<=2))
            rfm_comercios[i]["grupo_rfm"]="PERDIDO";
      }
      return rfm_comercios;
    }

    function agregar_comercios_cero(comercios, rfm_comercios){
      var lista_comercios_existentes=[];
      for(var i=0; i<rfm_comercios.length; i++)
        lista_comercios_existentes.push(rfm_comercios[i]._id);
      let difference = comercios.filter(x => !lista_comercios_existentes.includes(x));
      console.log("difference lenght", difference.length);
      for(var c=0; c<difference.length; c++){
        rfm_comercios.push({"_id": difference[c], 
                            "ultimo_deposito": "",
                            "numero_deposito": 0,
                            "avg_deposito":0,
                            "days":0,
                            "monetary":0,
                            "frequency":0,
                            "recency":0,
                            "grupo_rfm": "NO ACTIVIDAD"
                          })
      }
      return rfm_comercios;
    }

    function agregar_comercios_cero_sin_actividad(comercios, rfm_comercios){
      var lista_comercios_existentes=[];
      for(var i=0; i<rfm_comercios.length; i++)
        lista_comercios_existentes.push(rfm_comercios[i]._id.idcomercio);
      let difference = comercios.filter(x => !lista_comercios_existentes.includes(x));

     console.log("difference lenght", difference.length);
      console.log("comercios existentes lenght", lista_comercios_existentes.length);
      console.log("comercios existentes", lista_comercios_existentes)

      
      for(var c=0; c<difference.length; c++){
        rfm_comercios.push({"_id": difference[c], 
                            "ultimo_deposito": "",
                            "numero_deposito": 0,
                            "avg_deposito":0,
                            "days":0,
                            "monetary":0,
                            "frequency":0,
                            "recency":0,
                            "grupo_rfm": "NO ACTIVIDAD"
                          })
      }
      return rfm_comercios;
    }


    exports.obtener_grupo_rfm = function(req, res, next, res_function){
      var dbo=mongo_connection.dbo;
      let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
      let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
      let provincia = req.query.provincia? req.query.provincia:'';
      let comercio = req.query.comercio? req.query.comercio:'';
      let ciudad = req.query.ciudad? req.query.ciudad:'';
      let barrio = req.query.barrio? req.query.barrio:'';
      let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                         "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                         "barrio": new RegExp('^' + capitalizeWords(barrio)),
                         "codigo_merchant": new RegExp('^' + comercio)};
        
      dbo.collection("comercio").distinct("id", querycomercio, 
        (function(err, result) {
          if (err) console.log(err);
          //console.log("RESULTTTT", result)
          let querytrx= {"fecha_deposito": { $lt : fecha_fin, $gte: fecha_inicio}, "id_comercio": { $in: result}};

          dbo.collection("pagocomercio").aggregate([{$match: querytrx}, 
                                                    {$group: {_id: "$id_comercio", 
                                                              "ultimo_deposito":{$max: "$fecha_deposito"},
                                                              "numero_deposito":{$sum: 1},
                                                              "avg_deposito":{$avg: "$valor"}}},
                                                    {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {
    
            if (err2) console.log(err2);
            var result2=getDays(resulttrx, fecha_fin);
            result2=getRFM(result2);
            result2=agregar_grupo_rfm(result2);
            result2=agregar_comercios_cero(result,result2);
            res_function(null,result2, req, res, next);
          });
        }));
    }

    exports.obtener_grupo_rfm_con_actividad = function(req, res, next, res_function){
      var dbo=mongo_connection.dbo;
      let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
      let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
      let provincia = req.query.provincia? req.query.provincia:'';
      let comercio = req.query.comercio? req.query.comercio:'';
      let ciudad = req.query.ciudad? req.query.ciudad:'';
      let barrio = req.query.barrio? req.query.barrio:'';
      let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                         "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                         "barrio": new RegExp('^' + capitalizeWords(barrio)),
                         "codigo_merchant": new RegExp('^' + comercio)};
        
      dbo.collection("comercio").distinct("id", querycomercio, 
        (function(err, result) {
          if (err) console.log(err);
          console.log("RESULTTTT", result)
          let querytrx= {"fecha_deposito": { $lt : fecha_fin, $gte: fecha_inicio}, "id_comercio": { $in: result}};

          dbo.collection("pagocomercio").aggregate([{$match: querytrx}, 
                                                    {$group: {_id: "$id_comercio", 
                                                              "ultimo_deposito":{$max: "$fecha_deposito"},
                                                              "numero_deposito":{$sum: 1},
                                                              "avg_deposito":{$avg: "$valor"}}},
                                                    {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {
    
            if (err2) console.log(err2);
            var result2=getDays(resulttrx, fecha_fin);
            result2=getRFM(result2);
            result2=agregar_grupo_rfm(result2);
            res_function(null,result2, req, res, next);
          });
        }));
    }

    function contar_comercios_grupo_rfm(rfm_comercios){
      var counter_rfm_groups={"CAMPEON":0, "LEAL":0, "LEAL EN POTENCIA":0, "NUEVO":0, "PROMETEDOR":0,
                              "NECESITA ATENCION":0, "POR DORMIRSE":0, "EN RIESGO":0, "NO PODEMOS PERDERLO":0, 
                              "DORMIDO":0, "PERDIDO":0, "NO ACTIVIDAD":0};
      for(var i=0; i<rfm_comercios.length; i++){
        console.log("dhgdjfhdjnd", rfm_comercios[i].grupo_rfm, "comercioo", rfm_comercios[i]._id);
        counter_rfm_groups[rfm_comercios[i].grupo_rfm]++;
        
      }
      return counter_rfm_groups;
    }

    exports.obtener_num_comercios_grupo_rfm = function(req, res, next, res_function){
      var dbo=mongo_connection.dbo;
      let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
      let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
      let provincia = req.query.provincia? req.query.provincia:'';
      let comercio = req.query.comercio? req.query.comercio:'';
      let ciudad = req.query.ciudad? req.query.ciudad:'';
      let barrio = req.query.barrio? req.query.barrio:'';
      let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                         "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                         "barrio": new RegExp('^' + capitalizeWords(barrio)),
                         "codigo_merchant": new RegExp('^' + comercio)};
        
      dbo.collection("comercio").distinct("id", querycomercio, 
        (function(err, result) {
          if (err) console.log(err);
          console.log("RESULTTTT", result)
          let querytrx= {"fecha_deposito": { $lt : fecha_fin, $gte: fecha_inicio}, "id_comercio": { $in: result}};

          dbo.collection("pagocomercio").aggregate([{$match: querytrx}, 
                                                    {$group: {_id: "$id_comercio", 
                                                              "ultimo_deposito":{$max: "$fecha_deposito"},
                                                              "numero_deposito":{$sum: 1},
                                                              "avg_deposito":{$avg: "$valor"}}},
                                                    {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {
    
            if (err2) console.log(err2);
            var result2=getDays(resulttrx, fecha_fin);
            result2=getRFM(result2);
            result2=agregar_grupo_rfm(result2);
            result2=agregar_comercios_cero(result,result2);
            result2=contar_comercios_grupo_rfm(result2);
            res_function(null,result2, req, res, next);
          });
        }));
    }

    exports.obtener_grupo_rfm_por_locacion = function(req, res, next, res_function){
      var dbo=mongo_connection.dbo;
      let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
      let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
      let provincia = req.query.provincia? req.query.provincia:'';
      let ciudad = req.query.ciudad? req.query.ciudad:'';
      let barrio = req.query.barrio? req.query.barrio:'';
      let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                         "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                         "barrio": new RegExp('^' + capitalizeWords(barrio))};
        
      dbo.collection("comercio").distinct("id", querycomercio, 
        (function(err, result) {
          if (err) console.log(err);
          console.log("RESULTTTT", result)
          let querytrx= {"fecha_deposito": { $lt : fecha_fin, $gte: fecha_inicio}, "id_comercio": { $in: result}};

          dbo.collection("pagocomercio").aggregate([{$match: querytrx}, 
                                                    {$group: {_id: {idcomercio:"$id_comercio",
                                                                    fecha_deposito: {$substr: ['$fecha_deposito', 0, 6]}}, 
                                                              "ultimo_deposito":{$max: "$fecha_deposito"},
                                                              "numero_deposito":{$sum: 1},
                                                              "avg_deposito":{$avg: "$valor"}}},
                                                    {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {
    
            if (err2) console.log(err2);
            //console.log("RESULTTTT222", resulttrx)
            let fechas_unicas_set = new Set();
            var new_resultado_fechas=[];
            for(var i=0; i<resulttrx.length; i++){
                fechas_unicas_set.add(resulttrx[i]._id.fecha_deposito);
                
              }
            var fechas_unicas=Array.from(fechas_unicas_set); 
            console.log("fechas_unicas", fechas_unicas);
            for(var k=0; k<fechas_unicas.length ; k++){
              var resultado_por_fecha=[];
              for(var j=0; j<resulttrx.length ; j++){
                if(resulttrx[j]._id.fecha_deposito==fechas_unicas[k]){
                  resultado_por_fecha.push(resulttrx[j]);
                }               
              }
              console.log("resultados por fecha", resultado_por_fecha);
              resultado_por_fecha=getDays(resultado_por_fecha, fecha_fin);
              resultado_por_fecha=getRFM(resultado_por_fecha);
              resultado_por_fecha=agregar_grupo_rfm(resultado_por_fecha);
              console.log("*******************************************************************************");
              console.log("RESULTADO POR FECHA", resultado_por_fecha);
              let n_trx=resultado_por_fecha.length;
              let dictionary_rfm= {"CAMPEON":0,
                                  "LEAL":0,
                                  "LEAL EN POTENCIA":0,
                                  "NUEVO":0,
                                  "PROMETEDOR":0,
                                  "NECESITA ATENCION":0,
                                  "POR DORMIRSE":0,
                                  "EN RIESGO":0,
                                  "NO PODEMOS PERDERLO":0,
                                  "DORMIDO":0,
                                  "PERDIDO":0,
                                  "NO ACTIVIDAD":0};
              for(var h=0; h<resultado_por_fecha.length; h++){
                if(resultado_por_fecha[h].grupo_rfm in dictionary_rfm){
                  dictionary_rfm[resultado_por_fecha[h].grupo_rfm]++;
                }
                else{
                  dictionary_rfm[resultado_por_fecha[h].grupo_rfm]=1;
                }
              }
              console.log("despues de resultado por fecha", dictionary_rfm);
              for (var key in dictionary_rfm){
                dictionary_rfm[key]=((dictionary_rfm[key]/n_trx)*100).toFixed(2);
              }

              dictionary_rfm["fecha"]=fechas_unicas[k];
              
              new_resultado_fechas.push(dictionary_rfm);
            }
            
            res_function(null,new_resultado_fechas, req, res, next);
          });
        }));
    }

    exports.obtener_grupo_rfm_por_locacion_sin_actividad = function(req, res, next, res_function){
      var dbo=mongo_connection.dbo;
      let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
      let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
      let provincia = req.query.provincia? req.query.provincia:'';
      let ciudad = req.query.ciudad? req.query.ciudad:'';
      let barrio = req.query.barrio? req.query.barrio:'';
      let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                         "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                         "barrio": new RegExp('^' + capitalizeWords(barrio))};
        
      dbo.collection("comercio").distinct("id", querycomercio, 
        (function(err, result) {
          if (err) console.log(err);
         //console.log("RESULTTTT", result)
          let querytrx= {"fecha_deposito": { $lt : fecha_fin, $gte: fecha_inicio}, "id_comercio": { $in: result}};

          dbo.collection("pagocomercio").aggregate([{$match: querytrx}, 
                                                    {$group: {_id: {idcomercio:"$id_comercio",
                                                                    fecha_deposito: {$substr: ['$fecha_deposito', 0, 6]}}, 
                                                              "ultimo_deposito":{$max: "$fecha_deposito"},
                                                              "numero_deposito":{$sum: 1},
                                                              "avg_deposito":{$avg: "$valor"}}},
                                                    {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {
    
            if (err2) console.log(err2);
            //console.log("RESULTTTT222", resulttrx)
            let fechas_unicas_set = new Set();
            var new_resultado_fechas=[];
            for(var i=0; i<resulttrx.length; i++){
                fechas_unicas_set.add(resulttrx[i]._id.fecha_deposito);
                
              }
            var fechas_unicas=Array.from(fechas_unicas_set); 
            //console.log("fechas_unicas", fechas_unicas);
            for(var k=0; k<fechas_unicas.length ; k++){
              var resultado_por_fecha=[];
              for(var j=0; j<resulttrx.length ; j++){
                if(resulttrx[j]._id.fecha_deposito==fechas_unicas[k]){
                  resultado_por_fecha.push(resulttrx[j]);
                }               
              }
              //console.log("resultados por fecha", resultado_por_fecha);
              resultado_por_fecha=getDays(resultado_por_fecha, fecha_fin);
              resultado_por_fecha=getRFM(resultado_por_fecha);
              resultado_por_fecha=agregar_grupo_rfm(resultado_por_fecha);
              resultado_por_fecha=agregar_comercios_cero_sin_actividad(result,resultado_por_fecha);
              console.log("*******************************************************************************");
              //console.log("RESULTADO POR FECHA", resultado_por_fecha);
              let n_trx=resultado_por_fecha.length;
              let dictionary_rfm= {"CAMPEON":0,
                                  "LEAL":0,
                                  "LEAL EN POTENCIA":0,
                                  "NUEVO":0,
                                  "PROMETEDOR":0,
                                  "NECESITA ATENCION":0,
                                  "POR DORMIRSE":0,
                                  "EN RIESGO":0,
                                  "NO PODEMOS PERDERLO":0,
                                  "DORMIDO":0,
                                  "PERDIDO":0,
                                  "NO ACTIVIDAD":0};
              for(var h=0; h<resultado_por_fecha.length; h++){
                if(resultado_por_fecha[h].grupo_rfm in dictionary_rfm){
                  dictionary_rfm[resultado_por_fecha[h].grupo_rfm]++;
                }
                else{
                  dictionary_rfm[resultado_por_fecha[h].grupo_rfm]=1;
                }
              }
              //console.log("despues de resultado por fecha", dictionary_rfm);
              
              dictionary_rfm["fecha"]=fechas_unicas[k];
              
              new_resultado_fechas.push(dictionary_rfm);
            }
            
            res_function(null,new_resultado_fechas, req, res, next);
          });
        }));
    }