var conection = require('./elasticsearh_conection');


exports.obtener_catalogo = function (req, res, next, res_function){
    let id_tabla = req.body.id_tabla?parseInt(req.body.id_tabla):0;
    let padres = req.body.padres?req.body.padres:[];
    let query = {"constant_score" : {
            "filter" : {
                "bool" : {"must" : []}
            }
        }
    };
    if(id_tabla !== 0 || padres.length !==0 ) {

        if (id_tabla && id_tabla!==0) {
            query["constant_score"]["filter"]["bool"]["must"].push({"term": {"id_tabla": id_tabla}});
        }
        if (padres && padres.length !==0) {
            let filtro_padres = {"bool":{"should":[]}};
            for(let i= 0; i < padres.length; i++){
                filtro_padres["bool"]["should"].push({"term": {"id_dependencia": parseInt(padres[i])}});
            }
            query["constant_score"]["filter"]["bool"]["must"].push(filtro_padres);
        }

        conection.client.search({
            "index": "catalogos",
            "type": "catalogos",
            "body": {
                "size": 1000,
                "query": query,
            }
        }).then(function (body) {
            let lista_catalogos = body.hits.hits;
            let lista_resultados = [];
            for (let i = 0; i < lista_catalogos.length; i++) {
                let catalogo = {
                    "id": lista_catalogos[i]["_source"]["id_catalogo"],
                    "descripcion": lista_catalogos[i]["_source"]["descripcion"]
                };
                lista_resultados.push(catalogo);
            }
            res_function(null, {"lista_catalogo": lista_resultados}, req, res, next);
        }, function (err) {
            console.log(err);
            res_function(err, null, req, res, next);
        }).catch(function (err) {
            console.log(err);
            res_function(err, null, req, res, next);
        });
    }else{
        res_function(null, {"lista_catalogo": []}, req, res, next);
    }
};