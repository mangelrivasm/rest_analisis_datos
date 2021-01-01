var conection = require('./elasticsearh_conection');
var util = require('../util_model');


exports.obtener_ventas_n_periodo = function (req, res, next, res_function) {
    let periodo = req.query.periodo?req.query.periodo:'';
    let tipo = req.query.tipo;
    let n = req.query.n?req.query.n:1;
    let tipo_valor = req.query.tipo_valor? req.query.tipo_valor: 'pvp';
    if(periodo === 'mensual' || periodo === 'anual') {
        let fechas_limite= null;
        let intervalo = null;
        if(periodo === 'mensual') {
            fechas_limite = util.obtener_limites_fechas_n_meses(n);
            intervalo = '1M';
        }else{
            fechas_limite = util.obtener_limites_fechas_n_anios(n);
            intervalo = '1y';
        }
        let query = {
            "constant_score": {
                "filter": {
                    "bool": {
                        "must": [{
                            "range": {
                                "fecha": {
                                    "gte": fechas_limite.fecha_inicio,
                                    "lte": fechas_limite.fecha_fin
                                }
                            }
                        }
                        ]
                    }
                }
            }
        };
        if (tipo) {
            query["constant_score"]["filter"]["bool"]["must"].push({"term": {"identificador.plataforma.clave": tipo}});
        }
        let campo_sumar = 'valor';
        if (tipo_valor === 'pvp') {
            campo_sumar = 'valor';
        } else if (tipo_valor === 'tiempo_aire') {
            campo_sumar = 'tiempo_aire';
        }
        conection.client.search({
            "index": "resumen_ventas_mensuales",
            "type": "resumen_ventas_mensuales",
            "body": {
                "size": 0,
                "query": query,
                "aggs": {
                    "fecha": {
                        "date_histogram": {
                            "field": "fecha",
                            "interval": intervalo,
                            "format": "yyyyMM",
                            "min_doc_count": 0,
                            "extended_bounds": {
                                "min": fechas_limite.fecha_inicio,
                                "max": fechas_limite.fecha_fin
                            },
                            "order": {"_key": "desc"}
                        },
                        "aggs": {
                            "total": {"sum": {"field": campo_sumar}},
                            "numero_transacciones": {"sum": {"field": "numero_transacciones"}}
                        }
                    }
                }
            }
        }).then(function (body) {
            let result_aggregation = body.aggregations;
            let ventas = [];
            let values = result_aggregation.fecha.buckets;
            let suma_ventas = 0;
            let suma_numero_transacciones = 0;
            for (let i = values.length - 1; i >= 0; i--) {
                let total = util.formato_dinero(values[i].total.value);
                let numero = values[i].numero_transacciones.value;
                let promedio = util.formato_dinero(values[i].total.value / values[i].numero_transacciones.value);
                let key = values[i].key_as_string;
                let anio = parseInt(key.substr(0, 4));
                let obj = {
                    "anio": anio, "total": total,
                    "numero_transacciones": numero, "valor_promedio": promedio
                };
                if(periodo === 'mensual') {
                    obj["mes"] = parseInt(key.substr(4, 2));
                }
                ventas.push(obj);
                suma_ventas = suma_ventas + total;
                suma_numero_transacciones = suma_numero_transacciones + numero;
            }
            let promedio_mes = util.formato_dinero(suma_ventas / values.length);
            let promedio_transaccion = util.formato_dinero(suma_ventas / suma_numero_transacciones);
            let result = {"ventas": ventas, "promedio": promedio_mes, "promedio_transaccion": promedio_transaccion};
            res_function(null, result, req, res, next);
        }, function (err) {
            res_function(err, null, req, res, next);
        }).catch(function (err) {
            res_function(err, null, req, res, next);
        });
    }else{
        let err = new Error('Periodo incorrecto');
        res_function(err, null, req, res, next);
    }
};

exports.obtener_ventas_por_region = function (req, res, next, res_function){
    let periodo = req.query.periodo?req.query.periodo:'';
    let n = req.query.n?req.query.n:1;
    let tipo = req.query.tipo;
    let tipo_valor = req.query.tipo_valor? req.query.tipo_valor: 'pvp';
    if(periodo === 'mensual' || periodo === 'anual') {
        let fechas_limite= null;
        let intervalo = null;
        if(periodo === 'mensual') {
            fechas_limite = util.obtener_limites_fechas_n_meses(n);
            intervalo = '1M';
        }else{
            fechas_limite = util.obtener_limites_fechas_n_anios(n);
            intervalo = '1y';
        }
        let query = {
            "constant_score": {
                "filter": {
                    "bool": {
                        "must": [{
                            "range": {
                                "fecha": {
                                    "gte": fechas_limite.fecha_inicio,
                                    "lte": fechas_limite.fecha_fin
                                }
                            }
                        }
                        ]
                    }
                }
            }
        };
        if (tipo) {
            query["constant_score"]["filter"]["bool"]["must"].push({"term": {"identificador.plataforma.clave": tipo}});
        }
        let campo_sumar = 'valor';
        if (tipo_valor === 'pvp') {
            campo_sumar = 'valor';
        } else if (tipo_valor === 'tiempo_aire') {
            campo_sumar = 'tiempo_aire';
        }
        conection.client.search({
            "index": "resumen_ventas_mensuales",
            "type": "resumen_ventas_mensuales",
            "body": {
                "size": 0,
                "query": query,
                "aggs": {
                    "fecha": {
                        "date_histogram": {
                            "field": "fecha",
                            "interval": intervalo,
                            "format": "yyyyMM",
                            "extended_bounds": {
                                "min": fechas_limite.fecha_inicio,
                                "max": fechas_limite.fecha_fin
                            },
                            "order": {"_key": "desc"}
                        },
                        "aggs": {
                            "total": {
                                "sum": {"field": campo_sumar}
                            },
                            "region": {
                                "terms": {"field": "region.id"},
                                "aggs": {
                                    "detalle_region": {"top_hits": {"size": 1, "_source": {"include": ['region']}}},
                                    "total": {"sum": {"field": "valor"}}
                                }
                            }
                        }
                    }
                }
            }
        }).then(function (body) {
            let result_aggregation = body.aggregations;
            let fechas = result_aggregation.fecha.buckets;
            let lista_regiones = [];
            for (let i = 0; i < fechas.length; i++) {
                let regiones = fechas[i].region.buckets;
                for (let j = 0; j < regiones.length; j++) {
                    let region = regiones[j].detalle_region.hits.hits[0]._source.region;
                    let aux = -1;
                    for (let k = 0; k < lista_regiones.length; k++) {
                        if (lista_regiones[k].id === region.id) {
                            aux = k;
                            break;
                        }
                    }
                    if (aux <= -1) {
                        lista_regiones.push(region);
                    }
                }
            }
            let ventas = [];
            for (let i = fechas.length - 1; i >= 0; i--) {
                let key = fechas[i].key_as_string;
                let anio = parseInt(key.substr(0, 4));
                let total = util.formato_dinero(fechas[i].total.value);
                let obj = {"anio": anio, "total": total};
                if(periodo === 'mensual'){
                    obj["mes"] = parseInt(key.substr(4, 2));
                }
                ventas.push(obj);
                for (let j = 0; j < lista_regiones.length; j++) {
                    obj[lista_regiones[j].id] = 0;
                }
                let regiones = fechas[i].region.buckets;
                for (let j = 0; j < regiones.length; j++) {
                    let key_region = regiones[j].key;
                    obj[key_region] = util.formato_dinero(regiones[j].total.value);
                }
            }
            let result = {"regiones": lista_regiones, "ventas": ventas};
            res_function(null, result, req, res, next);
        }, function (err) {
            res_function(err, null, req, res, next);
        }).catch(function (err) {
            res_function(err, null, req, res, next);
        });
    }else{
        let err = new Error('Periodo invalido');
        res_function(err, null, req, res, next);
    }
};

exports.obtener_ventas_por_provincia_periodo = function (req,res,next,res_function) {
    let periodo = req.query.periodo;
    let anio = req.query.anio?req.query.anio:0;
    let mes = req.query.mes?req.query.mes:0;
    let tipo_valor = req.query.tipo_valor? req.query.tipo_valor: 'pvp';
    let tipo = req.query.tipo;
    let query ={"constant_score" : {
            "filter" : {
                "bool" : {"must" : []}
            }
        }
    };
    if(tipo){
        query["constant_score"]["filter"]["bool"]["must"].push({ "term" : {"identificador.plataforma.clave" : tipo}});
    }
    query["constant_score"]["filter"]["bool"]["must"].push({ "term" : {"anio" :anio}});
    if(periodo === "mensual")
        query["constant_score"]["filter"]["bool"]["must"].push({ "term" : {"mes" :mes}});
    let campo_sumar = 'valor';
    if (tipo_valor === 'pvp') {
        campo_sumar = 'valor';
    } else if (tipo_valor === 'tiempo_aire') {
        campo_sumar = 'tiempo_aire';
    }
    conection.client.search({
        "index": "resumen_ventas_mensuales",
        "type": "resumen_ventas_mensuales",
        "body": {
            "size":0,
            "query": query,
            "aggs": {
                "total": {
                    "sum": {"field": campo_sumar}
                },
                "provincia": {
                    "terms": {"field": "provincia.id",
                        "order": {"total.value" : "desc"},
                        "size":200},
                    "aggs": {
                        "detalle_provincia": {"top_hits": {"size": 1, "_source": {"include": ['provincia']}}},
                        "total": {
                            "sum": {"field": campo_sumar}
                        }
                    }
                }
            }

        }
    }).then(function (body) {
        let result_aggregation = body.aggregations.provincia.buckets;
        let total= body.aggregations.total.value;
        let result = [];
        for(let i=0;i<result_aggregation.length;i++){
            let provincia = result_aggregation[i].detalle_provincia.hits.hits[0]._source.provincia;
            let id_provincia = provincia.id;
            let descripcion_provincia = provincia.descripcion;
            let total_provincia =util.formato_dinero(result_aggregation[i].total.value);
            let porcentaje_provincia =util.formato_dinero(total_provincia/total*100);
            let obj={"id":id_provincia,"descripcion":descripcion_provincia,
                "total":total_provincia, "porcentaje":porcentaje_provincia};
            result.push(obj);
        }
        res_function(null,result, req, res, next);
    }, function (err) {
        console.log(err);
        res_function(err, null, req, res, next);
    }).catch(function (err) {
        console.log(err);
        res_function(err, null, req, res, next);
    });
};

exports.obtener_ventas_por_ciudad_periodo = function (req,res,next,res_function) {
    let periodo = req.query.periodo;
    let anio = req.query.anio?req.query.anio:0;
    let mes = req.query.mes?req.query.mes:0;
    let tipo_valor = req.query.tipo_valor? req.query.tipo_valor: 'pvp';
    let tipo = req.query.tipo;
    let query ={"constant_score" : {
            "filter" : {
                "bool" : {"must" : []}
            }
        }
    };
    if(tipo){
        query["constant_score"]["filter"]["bool"]["must"].push({ "term" : {"identificador.plataforma.clave" : tipo}});
    }
    query["constant_score"]["filter"]["bool"]["must"].push({ "term" : {"anio" :anio}});
    if(periodo === "mensual")
        query["constant_score"]["filter"]["bool"]["must"].push({ "term" : {"mes" :mes}});
    let campo_sumar = 'valor';
    if (tipo_valor === 'pvp') {
        campo_sumar = 'valor';
    } else if (tipo_valor === 'tiempo_aire') {
        campo_sumar = 'tiempo_aire';
    }
    conection.client.search({
        "index": "resumen_ventas_mensuales",
        "type": "resumen_ventas_mensuales",
        "body": {
            "size":0,
            "query": query,
            "aggs": {
                "total": {
                    "sum": {"field": campo_sumar}
                },
                "ciudad": {
                    "terms": {"field": "ciudad.id",
                        "order": {"total.value" : "desc"},
                        "size":500},
                    "aggs": {
                        "detalle_ciudad": {"top_hits": {"size": 1, "_source": {"include": ['ciudad']}}},
                        "total": {
                            "sum": {"field": campo_sumar}
                        }
                    }
                }
            }

        }
    }).then(function (body) {
        let result_aggregation = body.aggregations.ciudad.buckets;
        let total= body.aggregations.total.value;
        let result = [];
        for(let i=0;i<result_aggregation.length;i++){
            let ciudad = result_aggregation[i].detalle_ciudad.hits.hits[0]._source.ciudad;
            let id_ciudad = ciudad.id;
            let descripcion = ciudad.descripcion;
            let total_ciudad =util.formato_dinero(result_aggregation[i].total.value);
            let porcentaje_ciudad = total_ciudad/total*100;
            let obj={"id":id_ciudad, "descripcion":descripcion,
                "total":total_ciudad, "porcentaje":porcentaje_ciudad};
            result.push(obj);
        }
        res_function(null,result, req, res, next);
    }, function (err) {
        res_function(err, null, req, res, next);
    }).catch(function (err) {
        res_function(err, null, req, res, next);
    });
};

exports.obtener_ventas_por_proveedor_por_plataforma_periodo = function (req, res, next, res_function) {
    let periodo = req.query.periodo;
    let anio = req.query.anio?req.query.anio:0;
    let mes = req.query.mes?req.query.mes:0;
    let tipo_valor = req.query.tipo_valor? req.query.tipo_valor: 'pvp';
    let query ={"constant_score" : {
            "filter" : {
                "bool" : {"must" : []}
            }
        }
    };
    if(periodo === "anual" || periodo === "mensual"){
        query["constant_score"]["filter"]["bool"]["must"].push({ "term" : {"anio" :anio}});
        if(periodo === "mensual")
            query["constant_score"]["filter"]["bool"]["must"].push({ "term" : {"mes" :mes}});
        let campo_sumar = 'valor';
        if (tipo_valor === 'pvp') {
            campo_sumar = 'valor';
        } else if (tipo_valor === 'tiempo_aire') {
            campo_sumar = 'tiempo_aire';
        }
        conection.client.search({
            "index": "resumen_ventas_mensuales",
            "type": "resumen_ventas_mensuales",
            "body": {
                "size":0,
                "query": query,
                "aggs": {
                    "total":{"sum": {"field": campo_sumar}},
                    "numero_transacciones":{"sum": {"field": "numero_transacciones"}},
                    "proveedores": {
                        "terms": {"field":"proveedor_real.id",
                                    "size":500,
                                    "order": {"total.value" : "desc"}},
                        "aggs": {
                            "detalle_proveedor": {"top_hits": {"size": 1, "_source": {"include": ['proveedor_real']}}},
                            "total":{"sum": {"field": campo_sumar}},
                            "numero_transacciones":{"sum": {"field": "numero_transacciones"}},
                            "plataformas":{
                                "terms": {"field":"identificador.plataforma.id",
                                            "size":500},
                                "aggs":{
                                    "detalle_plataforma": {"top_hits": {"size": 1, "_source": {"include": ['identificador.plataforma']}}},
                                    "total":{"sum": {"field": campo_sumar}},
                                    "numero_transacciones":{"sum": {"field": "numero_transacciones"}}
                                }
                            }
                        }
                    }
                }

            }
        }).then(function (body) {
            let total = util.formato_dinero(body.aggregations.total.value);
            let numero_transacciones = body.aggregations.numero_transacciones.value;
            let proveedores_consulta = body.aggregations.proveedores.buckets;
            let lista_proveedores = [];
            let lista_plataformas = [{"clave": "total", "descripcion": "TOTAL"}];
            let result = {};
            for(let i =0; i< proveedores_consulta.length; i++){
                let detalle_proveedor = proveedores_consulta[i].detalle_proveedor.hits.hits[0]["_source"]["proveedor_real"];
                let total_proveedor = util.formato_dinero(proveedores_consulta[i].total.value);
                let numero_transacciones_proveedor = proveedores_consulta[i].numero_transacciones.value;
                let plataformas_consulta = proveedores_consulta[i].plataformas.buckets;
                for(let j=0; j<plataformas_consulta.length;j++){
                    let detalle_plataforma = plataformas_consulta[j].detalle_plataforma.hits.hits[0]["_source"]["identificador"]["plataforma"];
                    let total_plataforma = util.formato_dinero(plataformas_consulta[j].total.value);
                    let numero_transacciones_plataforma = plataformas_consulta[j].numero_transacciones.value;
                    detalle_proveedor[detalle_plataforma.clave]= {"valor":total_plataforma,
                                                                    "numero_transacciones":numero_transacciones_plataforma};
                    let index_plataforma = -1;
                    for(let k= 0; k <lista_plataformas.length; k++){
                        if(lista_plataformas[k].clave === detalle_plataforma.clave){
                            index_plataforma =k;
                            break;
                        }
                    }
                    if(index_plataforma === -1)
                        lista_plataformas.push(detalle_plataforma)
                }
                detalle_proveedor["total"] = {"valor":total_proveedor,
                                                "numero_transacciones":numero_transacciones_proveedor};
                lista_proveedores.push(detalle_proveedor);
            }
            result["lista_proveedores"] = lista_proveedores;
            result["lista_plataformas"] = lista_plataformas;
            result["total"]= total;
            result["numero_transacciones"]=numero_transacciones;
            res_function(null,result, req, res, next);
        }, function (err) {
            res_function(err, null, req, res, next);
        }).catch(function (err) {
            res_function(err, null, req, res, next);
        });
    }else{
        let err = new Error('Error en el periodo');
        err.status = 500;
        res_function(err, null, req, res, next);
    }
};

exports.obtener_ventas_por_categoria_por_plataforma_periodo = function (req, res, next, res_function) {
    let periodo = req.query.periodo;
    let anio = req.query.anio?req.query.anio:0;
    let mes = req.query.mes?req.query.mes:0;
    let tipo_valor = req.query.tipo_valor? req.query.tipo_valor: 'pvp';
    let query ={"constant_score" : {
            "filter" : {
                "bool" : {"must" : []}
            }
        }
    };
    if(periodo === "anual" || periodo === "mensual"){
        query["constant_score"]["filter"]["bool"]["must"].push({ "term" : {"anio" :anio}});
        if(periodo === "mensual")
            query["constant_score"]["filter"]["bool"]["must"].push({ "term" : {"mes" :mes}});
        let campo_sumar = 'valor';
        if (tipo_valor === 'pvp') {
            campo_sumar = 'valor';
        } else if (tipo_valor === 'tiempo_aire') {
            campo_sumar = 'tiempo_aire';
        }
        conection.client.search({
            "index": "resumen_ventas_mensuales",
            "type": "resumen_ventas_mensuales",
            "body": {
                "size":0,
                "query": query,
                "aggs": {
                    "total":{"sum": {"field": campo_sumar}},
                    "numero_transacciones":{"sum": {"field": "numero_transacciones"}},
                    "categorias": {
                        "terms": {"field":"categoria_proveedor.id",
                            "size":500,
                            "order": {"total.value" : "desc"}},
                        "aggs": {
                            "detalle_categoria": {"top_hits": {"size": 1, "_source": {"include": ['categoria_proveedor']}}},
                            "total":{"sum": {"field":campo_sumar}},
                            "numero_transacciones":{"sum": {"field": "numero_transacciones"}},
                            "plataformas":{
                                "terms": {"field":"identificador.plataforma.id",
                                    "size":500},
                                "aggs":{
                                    "detalle_plataforma": {"top_hits": {"size": 1, "_source": {"include": ['identificador.plataforma']}}},
                                    "total":{"sum": {"field": campo_sumar}},
                                    "numero_transacciones":{"sum": {"field": "numero_transacciones"}}
                                }
                            }
                        }
                    }
                }

            }
        }).then(function (body) {
            let total = util.formato_dinero(body.aggregations.total.value);
            let numero_transacciones = body.aggregations.numero_transacciones.value;
            let categorias_consulta = body.aggregations.categorias.buckets;
            let lista_categorias = [];
            let lista_plataformas = [{"clave": "total", "descripcion": "TOTAL"}];
            let result = {};
            for(let i =0; i< categorias_consulta.length; i++){
                let detalle_categoria = categorias_consulta[i].detalle_categoria.hits.hits[0]["_source"]["categoria_proveedor"];
                let total_categoria = util.formato_dinero(categorias_consulta[i].total.value);
                let numero_transacciones_categoria = categorias_consulta[i].numero_transacciones.value;
                let plataformas_consulta = categorias_consulta[i].plataformas.buckets;
                for(let j=0; j<plataformas_consulta.length;j++){
                    let detalle_plataforma = plataformas_consulta[j].detalle_plataforma.hits.hits[0]["_source"]["identificador"]["plataforma"];
                    let total_plataforma = util.formato_dinero(plataformas_consulta[j].total.value);
                    let numero_transacciones_plataforma = plataformas_consulta[j].numero_transacciones.value;
                    detalle_categoria[detalle_plataforma.clave]= {"valor":total_plataforma,
                        "numero_transacciones":numero_transacciones_plataforma};
                    let index_plataforma = -1;
                    for(let k= 0; k <lista_plataformas.length; k++){
                        if(lista_plataformas[k].clave === detalle_plataforma.clave){
                            index_plataforma =k;
                            break;
                        }
                    }
                    if(index_plataforma === -1)
                        lista_plataformas.push(detalle_plataforma)
                }
                detalle_categoria["total"] = {"valor":total_categoria,
                    "numero_transacciones":numero_transacciones_categoria};
                lista_categorias.push(detalle_categoria);
            }
            result["lista_categorias"] = lista_categorias;
            result["lista_plataformas"] = lista_plataformas;
            result["total"]= total;
            result["numero_transacciones"]=numero_transacciones;
            res_function(null,result, req, res, next);
        }, function (err) {
            res_function(err, null, req, res, next);
        }).catch(function (err) {
            res_function(err, null, req, res, next);
        });
    }else{
        let err = new Error('Error en el periodo');
        err.status = 500;
        res_function(err, null, req, res, next);
    }
};

exports.obtener_ventas_por_modelo_negocio_periodo = function (req, res, next, res_function) {
    let anio = req.query.anio?req.query.anio:0;
    let mes = req.query.mes?req.query.mes:0;
    let periodo = req.query.periodo? req.query.periodo:"";
    let tipo = req.query.tipo;
    let tipo_valor = req.query.tipo_valor? req.query.tipo_valor: 'pvp';
    let query ={"constant_score" : {
            "filter" : {
                "bool" : {"must" : []}
            }
        }
    };
    if(periodo === "anual" || periodo === "mensual") {
        if (tipo) {
            query["constant_score"]["filter"]["bool"]["must"].push({"term": {"identificador.plataforma.clave": tipo}});
        }
        query["constant_score"]["filter"]["bool"]["must"].push({"term": {"anio": anio}});
        if(periodo === "mensual") {
            query["constant_score"]["filter"]["bool"]["must"].push({"term": {"mes": mes}});
        }
        let campo_sumar = 'valor';
        if (tipo_valor === 'pvp') {
            campo_sumar = 'valor';
        } else if (tipo_valor === 'tiempo_aire') {
            campo_sumar = 'tiempo_aire';
        }
        conection.client.search({
            "index": "resumen_ventas_mensuales",
            "type": "resumen_ventas_mensuales",
            "body": {
                "size": 0,
                "query": query,
                "aggs": {
                    "total": {
                        "sum": {"field": campo_sumar}
                    },
                    "modelos_negocio": {
                        "terms": {
                            "field": "modelo_negocio.id",
                            "order": {"total.value": "desc"},
                            "size": 200
                        },
                        "aggs": {
                            "detalle_provincia": {"top_hits": {"size": 1, "_source": {"include": ['modelo_negocio']}}},
                            "total": {
                                "sum": {"field": campo_sumar}
                            }
                        }
                    }
                }

            }
        }).then(function (body) {
            let result_aggregation = body.aggregations.modelos_negocio.buckets;
            let total = body.aggregations.total.value;
            let result = [];
            for (let i = 0; i < result_aggregation.length; i++) {
                let modelo_negocio = result_aggregation[i].detalle_provincia.hits.hits[0]._source.modelo_negocio;
                let id_modelo_negocio = modelo_negocio.id;
                let descripcion_modelo_negocio = modelo_negocio.descripcion;
                let total_modelo_negocio = util.formato_dinero(result_aggregation[i].total.value);
                let porcentaje_modelo_negocio = util.formato_dinero(total_modelo_negocio / total * 100);
                let obj = {
                    "id": id_modelo_negocio, "descripcion": descripcion_modelo_negocio,
                    "total": total_modelo_negocio, "porcentaje": porcentaje_modelo_negocio
                };
                result.push(obj);
            }
            res_function(null, result, req, res, next);
        }, function (err) {
            console.log(err);
            res_function(err, null, req, res, next);
        }).catch(function (err) {
            console.log(err);
            res_function(err, null, req, res, next);
        });
    }else{
        let err = new Error('Error en el periodo');
        err.status = 500;
        res_function(err, null, req, res, next);
    }
};

exports.obtener_ventas_por_cuadrante = function (req, res, next, res_function) {
    let bounding_box = req.body.bounding_box;
    let modelos_negocio = req.body.modelos_negocio?req.body.modelos_negocio : [] ;
    let medios_venta = req.body.medios_venta?req.body.medios_venta:[];
    let periodo = req.body.periodo?req.body.periodo:"";
    let anio = req.body.anio? req.body.anio: 0;
    let mes = req.body.mes? req.body.mes: 0;
    let plataformas = req.body.plataformas?req.body.plataformas:[];
    let tipos_cliente = req.body.tipos_cliente?req.body.tipos_cliente:[];
    let ejecutivo = req.body.ejecutivo_venta?req.body.ejecutivo_venta:0;
    let south_west= bounding_box['south_west'];
    let north_east= bounding_box['north_east'];
    let filtro_cuadrante = {"geo_bounding_box": {
            "ubicacion": {
                "top_left": {"lon": south_west["lon"], "lat":north_east["lat"]},
                "bottom_right": {"lon":  north_east["lon"], "lat": south_west["lat"]}
            }
        }
    };
    let query ={"constant_score" : {
        "filter" : {
            "bool" : {"must" : [filtro_cuadrante]}
            }
        }
    };
    if(periodo === "anual") {
        query["constant_score"]["filter"]["bool"]["must"].push({"term": {"anio": anio}});
    }
    if(periodo === "mensual"){
        query["constant_score"]["filter"]["bool"]["must"].push({"term": {"anio": anio}});
        query["constant_score"]["filter"]["bool"]["must"].push({"term": {"mes": mes}});
    }
    if(plataformas.length > 0){
        let filtro_plataformas = {"bool":{"should":[]}};
        for(let i= 0; i < plataformas.length; i++){
            filtro_plataformas["bool"]["should"].push({"term": {"identificador.plataforma.clave": plataformas[i]}});
        }
        query["constant_score"]["filter"]["bool"]["must"].push(filtro_plataformas);
    }
    if(modelos_negocio.length > 0){
        let filtro_modelo_negocio = {"bool":{"should":[]}};
        for(let i = 0; i < modelos_negocio.length; i++){
            filtro_modelo_negocio["bool"]["should"].push({"term": {"modelo_negocio.clave": modelos_negocio[i]}});
        }
        query["constant_score"]["filter"]["bool"]["must"].push(filtro_modelo_negocio);
    }
    if(medios_venta.length > 0){
        let filtro_medios_venta ={"bool":{"should":[]}};
        for(let i =0; i< medios_venta.length; i++){
            filtro_medios_venta["bool"]["should"].push({"term": {"medio_venta.id": medios_venta[i]}});
        }
        query["constant_score"]["filter"]["bool"]["must"].push(filtro_medios_venta);
    }
    if(tipos_cliente.length > 0){
        let filtro_tipos_cliente = {"bool":{"should":[]}};
        for(let i =0; i<tipos_cliente.length; i++){
            filtro_tipos_cliente["bool"]["should"].push({"term": {"identificador.tipo.clave": tipos_cliente[i]}});
        }
        query["constant_score"]["filter"]["bool"]["must"].push(filtro_tipos_cliente);
    }
    if(ejecutivo != 0){
        query["constant_score"]["filter"]["bool"]["must"].push({"term": {"ejecutivo.id": ejecutivo}});
    }
    conection.client.search({
        "index": "resumen_ventas_mensuales",
        "type": "resumen_ventas_mensuales",
        "body": {
            "size": 0,
            "query": query,
            "aggs": {
                "total": {
                    "sum": {"field": "valor"}
                },
                "tiempo_aire": {
                    "sum": {"field": "tiempo_aire"}
                },
                "numero_transacciones": {
                    "sum": {"field": "numero_transacciones"}
                },
                "comercios": {
                    "terms": {
                        "field": "identificador.iso",
                        "order": {"total.value": "desc"},
                        "size": 10000
                    },
                    "aggs": {
                        "identificador": {"top_hits": {"size": 1, "_source": {"include": ['identificador']}}},
                        "ubicacion": {"top_hits": {"size": 1, "_source": {"include": ['ubicacion']}}},
                        "total": {
                            "sum": {"field": "valor"}
                        },
                        "tiempo_aire": {
                            "sum": {"field": "tiempo_aire"}
                        },
                        "numero_transacciones": {
                            "sum": {"field": "numero_transacciones"}
                        }
                    }
                }
            }

        }
    }).then(function obtener_pedazo_cuadrante (body) {
        let comercios = body.aggregations.comercios.buckets;
        let total = body.aggregations.total.value;
        let numero_transacciones = body.aggregations.numero_transacciones.value;
        let puntos_ventas = [];
        for(let i =0; i< comercios.length; i++) {
            let total_punto_venta = comercios[i].total.value;
            let tiempo_aire_punto_venta = comercios[i].tiempo_aire.value;
            let numero_transacciones_punto_venta = comercios[i].numero_transacciones.value;
            let ubicacion_punto_venta = comercios[i].ubicacion.hits.hits[0]["_source"].ubicacion;
            let identificacion_punto_venta = comercios[i].identificador.hits.hits[0]["_source"].identificador;
            let punto_venta = {"total": total_punto_venta, "numero_transacciones": numero_transacciones_punto_venta,
                                "ubicacion": ubicacion_punto_venta, "identificador":identificacion_punto_venta,
                                "tiempo_aire":tiempo_aire_punto_venta};
            puntos_ventas.push(punto_venta);
        }
        let result = {"total": total, "numero_transacciones": numero_transacciones,
                        "puntos_venta": puntos_ventas};
        res_function(null, result, req, res, next);
    }, function (err) {
        res_function(err, null, req, res, next);
    }).catch(function (err) {
        res_function(err, null, req, res, next);
    });
};

// numero puntos de venta
exports.obtener_numero_puntos_venta_mensual = function (req, res, next, res_function) {
    let fecha_inicio = req.body.fecha_inicio? req.body.fecha_inicio: '';
    let fecha_fin = req.body.fecha_fin? req.body.fecha_fin: '' ;
    let plataformas = req.body.plataformas? req.body.plataformas:[];
    let regiones = req.body.regiones? req.body.regiones:[];
    let provincias = req.body.provincias?req.body.provincias:[];
    let canales_cliente = req.body.canales_cliente? req.body.canales_cliente:[];
    let ejecutivo = req.body.ejecutivo? req.body.ejecutivo:0;
    let resultado_validacion_meses = util.validar_periodo_mensual(fecha_inicio, fecha_fin);
    if(resultado_validacion_meses.numero_meses >= 1) {
        let query = {
            "constant_score": {
                "filter": {
                    "bool": {
                        "must": [{"range": {"fecha": {"gte": resultado_validacion_meses.texto_fecha_inicio.substr(0,6),
                            "lte": resultado_validacion_meses.texto_fecha_fin.substr(0,6)}}}]
                    }
                }
            }
        };
        if(ejecutivo > 0){
            query["constant_score"]["filter"]["bool"]["must"].push({"term": {"ejecutivo.id":ejecutivo}});
        }

        if(plataformas.length > 0){
            let filtro_plataformas = {"bool":{"should":[]}};
            for(let i= 0; i < plataformas.length; i++){
                filtro_plataformas["bool"]["should"].push({"term": {"identificador.plataforma.clave": plataformas[i]}});
            }
            query["constant_score"]["filter"]["bool"]["must"].push(filtro_plataformas);
        }

        if(provincias.length > 0){
            let filtro_provincia = {"bool":{"should":[]}};
            for(let i= 0; i < provincias.length; i++){
                filtro_provincia["bool"]["should"].push({"term": {"provincia.id": provincias[i]}});
            }
            query["constant_score"]["filter"]["bool"]["must"].push(filtro_provincia);
        }else if( regiones.length > 0){
            let filtro_regiones = {"bool":{"should":[]}};
            for(let i= 0; i < regiones.length; i++){
                filtro_regiones["bool"]["should"].push({"term": {"region.id": regiones[i]}});
            }
            query["constant_score"]["filter"]["bool"]["must"].push(filtro_regiones);
        }

        if(canales_cliente.length > 0){
            let filtros_canales_cliente = {"bool":{"should":[]}};
            for(let i =0; i < canales_cliente.length; i++){
                filtros_canales_cliente["bool"]["should"].push({"term": {"canal_cliente.id": canales_cliente[i]}});
            }
            query["constant_score"]["filter"]["bool"]["must"].push(filtros_canales_cliente);
        }

        conection.client.search({
            "index": "resumen_ventas_mensuales",
            "type": "resumen_ventas_mensuales",
            "body": {
                "size": 0,
                "query": query,
                "aggs": {
                    "meses": {
                        "date_histogram" : {
                            "field" : "fecha",
                            "interval" : "1M",
                            "format" : "yyyyMM",
                            "min_doc_count": 0,
                            "extended_bounds" : {
                                "min" : resultado_validacion_meses.texto_fecha_inicio.substr(0,6),
                                "max" : resultado_validacion_meses.texto_fecha_fin.substr(0,6)
                            },
                            "order": {"_key": "asc"}
                        },
                        "aggs": {
                            "numero_puntos_venta": {
                                "cardinality": {
                                    "field": "identificador.iso"
                                }
                            }
                        }
                    }
                }
            }
        }).then(function (body) {
            let puntos_venta_meses = body.aggregations.meses.buckets;
            let list_puntos_venta_mes = [];
            for(let i=0; i< puntos_venta_meses.length; i++){
                let puntos_venta_mes = puntos_venta_meses[i];
                let key = puntos_venta_mes.key_as_string;
                let numero_puntos_venta = puntos_venta_mes.numero_puntos_venta.value;
                let anio = parseInt(key.substr(0,4));
                let mes = parseInt(key.substr(4,2));
                let obj = {"anio": anio,
                    "mes": mes,
                    "numero_puntos_venta":numero_puntos_venta };
                list_puntos_venta_mes.push(obj);
            }
            res_function(null, {"lista_puntos_venta": list_puntos_venta_mes}, req, res, next);
        }, function (err) {
            console.log(err);
            res_function(err, null, req, res, next);
        }).catch(function (err) {
            console.log(err);
            res_function(err, null, req, res, next);
        });
    }else{
        let err = new Error('Error Fechas invalidas');
        err.status = 500;
        res_function(err, null, req, res, next);
    }
};

exports.obtener_numero_puntos_venta_nuevos_mensual = function (req, res, next, res_function) {
    let fecha_inicio = req.body.fecha_inicio? req.body.fecha_inicio: '';
    let fecha_fin = req.body.fecha_fin? req.body.fecha_fin: '' ;
    let plataformas = req.body.plataformas? req.body.plataformas:[] ;
    let regiones = req.body.regiones? req.body.regiones:[];
    let provincias = req.body.provincias?req.body.provincias:[];
    let canales_cliente = req.body.canales_cliente? req.body.canales_cliente:[];
    let ejecutivo = req.body.ejecutivo? req.body.ejecutivo:0;

    let resultado_validacion_meses = util.validar_periodo_mensual(fecha_inicio, fecha_fin);
    if(resultado_validacion_meses.numero_meses >= 1) {
        let query = {
            "constant_score": {
                "filter": {
                    "bool": {
                        "must": [{"range": {"fecha_ingreso": {"gte": resultado_validacion_meses.texto_fecha_inicio,
                            "lte": resultado_validacion_meses.texto_fecha_fin}}}]
                    }
                }
            }
        };

        if(ejecutivo > 0){
            query["constant_score"]["filter"]["bool"]["must"].push({"term": {"id_ejecutivo":ejecutivo}});
        }

        if(plataformas.length > 0){
            let filtro_plataformas = {"bool":{"should":[]}};
            for(let i= 0; i < plataformas.length; i++){
                filtro_plataformas["bool"]["should"].push({"term": {"tipo.clave": plataformas[i]}});
            }
            query["constant_score"]["filter"]["bool"]["must"].push(filtro_plataformas);
        }

        if(provincias.length > 0){
            let filtro_provincia = {"bool":{"should":[]}};
            for(let i= 0; i < provincias.length; i++){
                filtro_provincia["bool"]["should"].push({"term": {"id_provincia": provincias[i]}});
            }
            query["constant_score"]["filter"]["bool"]["must"].push(filtro_provincia);
        }else if( regiones.length > 0){
            let filtro_regiones = {"bool":{"should":[]}};
            for(let i= 0; i < regiones.length; i++){
                filtro_regiones["bool"]["should"].push({"term": {"id_region": regiones[i]}});
            }
            query["constant_score"]["filter"]["bool"]["must"].push(filtro_regiones);
        }

        if(canales_cliente.length > 0){
            let filtros_canales_cliente = {"bool":{"should":[]}};
            for(let i =0; i < canales_cliente.length; i++){
                filtros_canales_cliente["bool"]["should"].push({"term": {"id_canal_venta": canales_cliente[i]}});
            }
            query["constant_score"]["filter"]["bool"]["must"].push(filtros_canales_cliente);
        }

        conection.client.search({
            "index": "puntos_venta",
            "type": "puntos_venta",
            "body": {
                "size": 0,
                "query": query,
                "aggs": {
                    "meses": {
                        "date_histogram" : {
                            "field" : "fecha_ingreso",
                            "interval" : "1M",
                            "format" : "yyyyMMdd",
                            "min_doc_count": 0,
                            "extended_bounds" : {
                                "min" : resultado_validacion_meses.texto_fecha_inicio,
                                "max" : resultado_validacion_meses.texto_fecha_fin
                            },
                            "order": {"_key": "asc"}
                        },
                        "aggs": {
                            "numero_puntos_venta": {
                                "cardinality": {
                                    "field": "iso"
                                }
                            }
                        }
                    }
                }
            }
        }).then(function (body) {
            let puntos_venta_meses = body.aggregations.meses.buckets;
            let list_puntos_venta_mes = [];
            for(let i=0; i< puntos_venta_meses.length; i++){
                let puntos_venta_mes = puntos_venta_meses[i];
                let key = puntos_venta_mes.key_as_string;
                let numero_puntos_venta = puntos_venta_mes.numero_puntos_venta.value;
                let anio = parseInt(key.substr(0,4));
                let mes = parseInt(key.substr(4,2));
                let obj = {"anio": anio,
                    "mes": mes,
                    "numero_puntos_venta_nuevos": numero_puntos_venta};
                list_puntos_venta_mes.push(obj);
            }
            res_function(null, {"lista_nuevos_puntos_venta": list_puntos_venta_mes}, req, res, next);
        }, function (err) {
            console.log(err);
            res_function(err, null, req, res, next);
        }).catch(function (err) {
            console.log(err);
            res_function(err, null, req, res, next);
        });
    }else{
        let err = new Error('Error Fechas invalidas');
        err.status = 500;
        res_function(err, null, req, res, next);
    }
};

exports.obtener_numero_puntos_venta_anual = function (req, res, next, res_function) {
    let fecha_inicio = req.body.fecha_inicio? req.body.fecha_inicio: '';
    let fecha_fin = req.body.fecha_fin? req.body.fecha_fin: '' ;
    let plataformas = req.body.plataformas? req.body.plataformas:[] ;
    let regiones = req.body.regiones? req.body.regiones:[];
    let provincias = req.body.provincias?req.body.provincias:[];
    let canales_cliente = req.body.canales_cliente? req.body.canales_cliente:[];
    let ejecutivo = req.body.ejecutivo? req.body.ejecutivo:0;
    let resultado_validacion_anios = util.validar_periodo_anual(fecha_inicio, fecha_fin);
    if(resultado_validacion_anios.numero_anios >= 1) {
        let query = {
            "constant_score": {
                "filter": {
                    "bool": {
                        "must": [{"range": {"fecha": {"gte": resultado_validacion_anios.texto_fecha_inicio.substr(0,6),
                                "lte": resultado_validacion_anios.texto_fecha_fin.substr(0,6)}}}]
                    }
                }
            }
        };
        if(ejecutivo > 0){
            query["constant_score"]["filter"]["bool"]["must"].push({"term": {"ejecutivo.id":ejecutivo}});
        }

        if(plataformas.length > 0){
            let filtro_plataformas = {"bool":{"should":[]}};
            for(let i= 0; i < plataformas.length; i++){
                filtro_plataformas["bool"]["should"].push({"term": {"identificador.plataforma.clave": plataformas[i]}});
            }
            query["constant_score"]["filter"]["bool"]["must"].push(filtro_plataformas);
        }

        if(provincias.length > 0){
            let filtro_provincia = {"bool":{"should":[]}};
            for(let i= 0; i < provincias.length; i++){
                filtro_provincia["bool"]["should"].push({"term": {"provincia.id": provincias[i]}});
            }
            query["constant_score"]["filter"]["bool"]["must"].push(filtro_provincia);
        }else if( regiones.length > 0){
            let filtro_regiones = {"bool":{"should":[]}};
            for(let i= 0; i < regiones.length; i++){
                filtro_regiones["bool"]["should"].push({"term": {"region.id": regiones[i]}});
            }
            query["constant_score"]["filter"]["bool"]["must"].push(filtro_regiones);
        }

        if(canales_cliente.length > 0){
            let filtros_canales_cliente = {"bool":{"should":[]}};
            for(let i =0; i < canales_cliente.length; i++){
                filtros_canales_cliente["bool"]["should"].push({"term": {"canal_cliente.id": canales_cliente[i]}});
            }
            query["constant_score"]["filter"]["bool"]["must"].push(filtros_canales_cliente);
        }

        conection.client.search({
            "index": "resumen_ventas_mensuales",
            "type": "resumen_ventas_mensuales",
            "body": {
                "size": 0,
                "query": query,
                "aggs": {
                    "meses": {
                        "date_histogram" : {
                            "field" : "fecha",
                            "interval" : "1y",
                            "format" : "yyyyMM",
                            "min_doc_count": 0,
                            "extended_bounds" : {
                                "min" : resultado_validacion_anios.texto_fecha_inicio.substr(0,6),
                                "max" : resultado_validacion_anios.texto_fecha_fin.substr(0,6)
                            },
                            "order": {"_key": "asc"}
                        },
                        "aggs": {
                            "numero_puntos_venta": {
                                "cardinality": {
                                    "field": "identificador.iso"
                                }
                            }
                        }
                    }
                }
            }
        }).then(function (body) {
            let puntos_venta_meses = body.aggregations.meses.buckets;
            let list_puntos_venta_mes = [];
            for(let i=0; i< puntos_venta_meses.length; i++){
                let puntos_venta_mes = puntos_venta_meses[i];
                let key = puntos_venta_mes.key_as_string;
                let numero_puntos_venta = puntos_venta_mes.numero_puntos_venta.value;
                let anio = parseInt(key.substr(0,4));
                let obj = {"anio": anio,
                    "numero_puntos_venta":numero_puntos_venta };
                list_puntos_venta_mes.push(obj);
            }
            res_function(null, {"lista_puntos_venta": list_puntos_venta_mes}, req, res, next);
        }, function (err) {
            console.log(err);
            res_function(err, null, req, res, next);
        }).catch(function (err) {
            console.log(err);
            res_function(err, null, req, res, next);
        });
    }else{
        let err = new Error('Error Fechas invalidas');
        err.status = 500;
        res_function(err, null, req, res, next);
    }
};

exports.obtener_numero_puntos_venta_nuevos_anual = function (req, res, next, res_function) {
    let fecha_inicio = req.body.fecha_inicio? req.body.fecha_inicio: '';
    let fecha_fin = req.body.fecha_fin? req.body.fecha_fin: '' ;
    let plataformas = req.body.plataformas? req.body.plataformas:[] ;
    let regiones = req.body.regiones? req.body.regiones:[];
    let provincias = req.body.provincias?req.body.provincias:[];
    let canales_cliente = req.body.canales_cliente? req.body.canales_cliente:[];
    let ejecutivo = req.body.ejecutivo? req.body.ejecutivo:0;

    let resultado_validacion_anios = util.validar_periodo_anual(fecha_inicio, fecha_fin);
    if(resultado_validacion_anios.numero_anios >= 1) {
        let query = {
            "constant_score": {
                "filter": {
                    "bool": {
                        "must": [{"range": {"fecha_ingreso": {"gte": resultado_validacion_anios.texto_fecha_inicio,
                            "lte": resultado_validacion_anios.texto_fecha_fin}}}]
                    }
                }
            }
        };

        if(ejecutivo > 0){
            query["constant_score"]["filter"]["bool"]["must"].push({"term": {"id_ejecutivo":ejecutivo}});
        }

        if(plataformas.length > 0){
            let filtro_plataformas = {"bool":{"should":[]}};
            for(let i= 0; i < plataformas.length; i++){
                filtro_plataformas["bool"]["should"].push({"match": {"tipo.clave": plataformas[i]}});
            }
            query["constant_score"]["filter"]["bool"]["must"].push(filtro_plataformas);
        }

        if(provincias.length > 0){
            let filtro_provincia = {"bool":{"should":[]}};
            for(let i= 0; i < provincias.length; i++){
                filtro_provincia["bool"]["should"].push({"term": {"id_provincia": provincias[i]}});
            }
            query["constant_score"]["filter"]["bool"]["must"].push(filtro_provincia);
        }else if( regiones.length > 0){
            let filtro_regiones = {"bool":{"should":[]}};
            for(let i= 0; i < regiones.length; i++){
                filtro_regiones["bool"]["should"].push({"term": {"id_region": regiones[i]}});
            }
            query["constant_score"]["filter"]["bool"]["must"].push(filtro_regiones);
        }

        if(canales_cliente.length > 0){
            let filtros_canales_cliente = {"bool":{"should":[]}};
            for(let i =0; i < canales_cliente.length; i++){
                filtros_canales_cliente["bool"]["should"].push({"term": {"id_canal_venta": canales_cliente[i]}});
            }
            query["constant_score"]["filter"]["bool"]["must"].push(filtros_canales_cliente);
        }

        conection.client.search({
            "index": "puntos_venta",
            "type": "puntos_venta",
            "body": {
                "size": 0,
                "query": query,
                "aggs": {
                    "meses": {
                        "date_histogram" : {
                            "field" : "fecha_ingreso",
                            "interval" : "1y",
                            "format" : "yyyyMMdd",
                            "min_doc_count": 0,
                            "extended_bounds" : {
                                "min" : resultado_validacion_anios.texto_fecha_inicio,
                                "max" : resultado_validacion_anios.texto_fecha_fin
                            },
                            "order": {"_key": "asc"}
                        },
                        "aggs": {
                            "numero_puntos_venta": {
                                "cardinality": {
                                    "field": "iso"
                                }
                            }
                        }
                    }
                }
            }
        }).then(function (body) {
            let puntos_venta_meses = body.aggregations.meses.buckets;
            let list_puntos_venta_mes = [];
            for(let i=0; i< puntos_venta_meses.length; i++){
                let puntos_venta_mes = puntos_venta_meses[i];
                let key = puntos_venta_mes.key_as_string;
                let numero_puntos_venta = puntos_venta_mes.numero_puntos_venta.value;
                let anio = parseInt(key.substr(0,4));
                let obj = {"anio": anio,
                    "numero_puntos_venta_nuevos": numero_puntos_venta};
                list_puntos_venta_mes.push(obj);
            }
            res_function(null, {"lista_nuevos_puntos_venta": list_puntos_venta_mes}, req, res, next);
        }, function (err) {
            console.log(err);
            res_function(err, null, req, res, next);
        }).catch(function (err) {
            console.log(err);
            res_function(err, null, req, res, next);
        });
    }else{
        let err = new Error('Error Fechas invalidas');
        err.status = 500;
        res_function(err, null, req, res, next);
    }
};

exports.obtener_ventas = function (req, res, next, res_function) {
    let periodo = req.query.periodo? req.query.periodo: 'mensual';
    let tipo_valor = req.query.tipo_valor? req.query.tipo_valor: 'pvp';
    let fecha_inicio = req.query.fecha_inicio?  req.query.fecha_inicio : '';
    let fecha_fin = req.query.fecha_fin?  req.query.fecha_fin : '';
    let iso = req.query.iso? req.query.iso: '';
    let resultado_validacion_fecha= {};
    let diferencia_fechas = -1;
    if(periodo === 'mensual'){
        resultado_validacion_fecha = util.validar_periodo_mensual(fecha_inicio, fecha_fin);
        diferencia_fechas = resultado_validacion_fecha.numero_meses;
    }
    else if(periodo === 'diario'){
        resultado_validacion_fecha = util.validar_periodo_diario(fecha_inicio, fecha_fin);
        diferencia_fechas = resultado_validacion_fecha.numero_dias;
    }

    if(periodo !== 'mensual' && periodo !== 'diario'){
        let err = new Error("Periodo no valido");
        res_function(err, null, req, res, next);
    }else if(diferencia_fechas < 0){
        let err = new Error("Rango de fechas incorrectos");
        res_function(err, null, req, res, next);
    }else if(tipo_valor !== "pvp" && tiempo_aire !=="tiempo_aire"){
        let err = new Error("Tipo de valor no valido");
        res_function(err, null, req, res, next);
    }else{
        let campo_sumar = 'valor';
        let intervalo = '1M';
        let format = 'yyyyMM';
        let index_ventas = "resumen_ventas_mensuales";
        if(periodo === 'diario') {
            intervalo = '1d';
            format = 'yyyyMMdd';
            index_ventas = "resumen_ventas_diarias";
        }
        if (tipo_valor === 'tiempo_aire') {
            campo_sumar = 'tiempo_aire';
        }
        let query = {
            "constant_score": {
                "filter": {
                    "bool": {
                        "must": [{
                            "range": {
                                "fecha": {
                                    "gte": fecha_inicio,
                                    "lte": fecha_fin
                                }
                            }
                        }
                        ]
                    }
                }
            }
        };
        if (iso) {
            query["constant_score"]["filter"]["bool"]["must"].push({"match": {"identificador.iso": iso}});
        }
        conection.client.search({
            "index": index_ventas,
            "type": index_ventas,
            "body": {
                "size": 0,
                "query": query,
                "aggs": {
                    "fechas": {
                        "date_histogram": {
                            "field": "fecha",
                            "interval": intervalo,
                            "format": format,
                            "min_doc_count": 0,
                            "extended_bounds": {
                                "min": fecha_inicio,
                                "max": fecha_fin
                            },
                            "order": {"_key": "desc"}
                        },
                        "aggs": {
                            "total": {"sum": {"field": campo_sumar}},
                            "numero_transacciones": {"sum": {"field": "numero_transacciones"}}
                        }
                    }
                }
            }
        }).then(function (body) {
            let result_aggregation = body.aggregations;
            let ventas = [];
            let values = result_aggregation.fechas.buckets;
            let suma_ventas = 0;
            let suma_numero_transacciones = 0;
            for (let i = values.length - 1; i >= 0; i--) {
                let total = util.formato_dinero(values[i].total.value);
                let numero = values[i].numero_transacciones.value;
                let promedio = util.formato_dinero(values[i].total.value / values[i].numero_transacciones.value);
                let fecha = values[i].key_as_string;
                let obj = {
                    "fecha": fecha,
                    "total": total,
                    "numero_transacciones": numero,
                    "valor_promedio": promedio
                };
                ventas.push(obj);
                suma_ventas = suma_ventas + total;
                suma_numero_transacciones = suma_numero_transacciones + numero;
            }
            let promedio_ventas = util.formato_dinero(suma_ventas / values.length);
            let promedio_transaccion = util.formato_dinero(suma_ventas / suma_numero_transacciones);
            let result = {"ventas": ventas,
                "promedio": promedio_ventas,
                "promedio_transaccion": promedio_transaccion};
            res_function(null, result, req, res, next);
        }, function (err) {
            res_function(err, null, req, res, next);
        }).catch(function (err) {
            res_function(err, null, req, res, next);
        });
    }

};


exports.obtener_ventas_etiqueta_proveedor = function (req, res, next, res_function) {
    let fecha_inicio = req.query.fecha_inicio?  req.query.fecha_inicio : '';
    let fecha_fin = req.query.fecha_fin?  req.query.fecha_fin : '';
    let tipo_valor = req.query.tipo_valor? req.query.tipo_valor: "pvp";
    let tipo_categoria = req.query.tipo_categoria? req.query.tipo_categoria: "producto";
    let iso = req.query.iso? req.query.iso: '';
    let resultado_validacion_meses = util.validar_periodo_mensual(fecha_inicio, fecha_fin);
    if(resultado_validacion_meses.numero_meses < 0){
        let err = new Error("Rango de fechas incorrectos  ");
        res_function(err, null, req, res, next);
    }else if(tipo_valor !== "pvp" && tiempo_aire !=="tiempo_aire"){
        let err = new Error("Tipo de valor no valido");
        res_function(err, null, req, res, next);
    }else if(tipo_categoria!== "producto" && tipo_categoria !=="categoria"){
        let err = new Error("Categoria no valida");
        res_function(err, null, req, res, next);
    }else{
        let query = {
            "constant_score": {
                "filter": {
                    "bool": {
                        "must": [{
                            "range": {
                                "fecha": {
                                    "gte": fecha_inicio,
                                    "lte": fecha_fin
                                }
                            }
                        }
                        ]
                    }
                }
            }
        };
        let campo_sumar = "valor";
        if (tipo_valor === "tiempo_aire") {
            campo_sumar = "tiempo_aire";
        }
        let campo_categoria = "proveedor_real";
        if (tipo_categoria === "categoria"){
            campo_categoria = "categoria_proveedor";
        }
        if (iso) {
            query["constant_score"]["filter"]["bool"]["must"].push({"match": {"identificador.iso": iso}});
        }
        conection.client.search({
            "index": "resumen_ventas_mensuales",
            "type": "resumen_ventas_mensuales",
            "body": {
                "size":0,
                "query": query,
                "aggs": {
                    "total":{"sum": {"field": campo_sumar}},
                    "numero_transacciones":{"sum": {"field": "numero_transacciones"}},
                    "categorias": {
                        "terms": {"field": campo_categoria+".id",
                            "size":500,
                            "order": {"total.value" : "desc"}},
                        "aggs": {
                            "detalle_categoria": {"top_hits": {"size": 1, "_source": {"include": [campo_categoria]}}},
                            "total":{"sum": {"field":campo_sumar}},
                            "numero_transacciones":{"sum": {"field": "numero_transacciones"}},
                            "plataformas":{
                                "terms": {"field":"identificador.plataforma.id",
                                    "size":500},
                                "aggs":{
                                    "detalle_plataforma": {"top_hits": {"size": 1, "_source": {"include": ['identificador.plataforma']}}},
                                    "total":{"sum": {"field": campo_sumar}},
                                    "numero_transacciones":{"sum": {"field": "numero_transacciones"}},
                                    "fechas":{
                                        "date_histogram": {
                                            "field": "fecha",
                                            "interval": '1M',
                                            "format": 'yyyyMM',
                                            "min_doc_count": 0,
                                            "extended_bounds": {
                                                "min": fecha_inicio,
                                                "max": fecha_fin
                                            },
                                            "order": {"_key": "asc"}
                                        },
                                        "aggs": {
                                            "total": {"sum": {"field": campo_sumar}},
                                            "numero_transacciones": {"sum": {"field": "numero_transacciones"}}
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "fechas":{
                        "date_histogram": {
                            "field": "fecha",
                            "interval": '1M',
                            "format": 'yyyyMM',
                            "min_doc_count": 0,
                            "extended_bounds": {
                                "min": fecha_inicio,
                                "max": fecha_fin
                            },
                            "order": {"_key": "asc"}
                        },
                        "aggs": {
                            "total": {"sum": {"field": campo_sumar}},
                            "numero_transacciones": {"sum": {"field": "numero_transacciones"}}
                        }
                    },
                    "plataformas": {
                        "terms": {
                            "field": "identificador.plataforma.id",
                            "size": 500
                        },
                        "aggs": {
                            "detalle_plataforma": {
                                "top_hits": {
                                    "size": 1,
                                    "_source": {"include": ['identificador.plataforma']}
                                }
                            },
                            "total": {"sum": {"field": campo_sumar}},
                            "numero_transacciones": {"sum": {"field": "numero_transacciones"}}
                        }
                    }
                }
            }
        }).then(function (body) {
            let total = util.formato_dinero(body.aggregations.total.value);
            let numero_transacciones = body.aggregations.numero_transacciones.value;
            let categorias_consulta = body.aggregations.categorias.buckets;
            let fechas_consulta = body.aggregations.fechas.buckets;
            let lista_plataformas_consultas = body.aggregations.plataformas.buckets;
            let lista_fechas = [];
            let lista_categorias = [];
            let lista_plataformas = [];
            let result = {};

            for(let i=0; i < fechas_consulta.length; i++){
                let valores_fecha = fechas_consulta[i];
                lista_fechas.push({
                    "fecha": valores_fecha.key_as_string,
                    "valor": util.formato_dinero(valores_fecha.total.value),
                    "numero_transacciones": valores_fecha.numero_transacciones.value
                });
            }

            for(let i =0; i < lista_plataformas_consultas.length; i++){
                lista_plataformas.push(lista_plataformas_consultas[i].detalle_plataforma.hits.hits[0]["_source"]["identificador"]["plataforma"])
            }

            for(let i =0; i< categorias_consulta.length; i++){
                let categoria = categorias_consulta[i];
                let detalle_categoria = categoria.detalle_categoria.hits.hits[0]["_source"][campo_categoria];
                let total_categoria = util.formato_dinero(categoria.total.value);
                let numero_transacciones_categoria = categoria.numero_transacciones.value;
                let plataformas_consulta = categoria.plataformas.buckets;
                detalle_categoria["valor"] = total_categoria;
                detalle_categoria["numero_transacciones"] = numero_transacciones_categoria;

                for(let j=0; j<plataformas_consulta.length;j++){
                    let plataforma = plataformas_consulta[j];
                    let clave_plataforma = plataforma.detalle_plataforma.hits.hits[0]["_source"]["identificador"]["plataforma"]["clave"];
                    let total_plataforma = util.formato_dinero(plataforma.total.value);
                    let numero_transacciones_plataforma = plataforma.numero_transacciones.value;
                    detalle_categoria[clave_plataforma] = {"valor":total_plataforma,
                        "numero_transacciones":numero_transacciones_plataforma,
                        "fechas": []};
                    let fechas_categoria = plataforma.fechas.buckets;
                    for(let k= 0; k< fechas_categoria.length; k++){
                        let valores_fecha = fechas_categoria[k];
                        detalle_categoria[clave_plataforma]["fechas"].push({
                            "fecha": valores_fecha.key_as_string,
                            "valor": util.formato_dinero(valores_fecha.total.value),
                            "numero_transacciones": valores_fecha.numero_transacciones.value
                        });
                    }
                }

                lista_categorias.push(detalle_categoria);
            }
            result["totales"] = lista_fechas;
            result["lista_categorias"] = lista_categorias;
            result["lista_plataformas"] = lista_plataformas;
            result["total"]= total;
            result["numero_transacciones"]=numero_transacciones;
            res_function(null,result, req, res, next);
            //res_function(null,body, req, res, next);
        }, function (err) {
            res_function(err, null, req, res, next);
        }).catch(function (err) {
            res_function(err, null, req, res, next);
        });
    }

};
