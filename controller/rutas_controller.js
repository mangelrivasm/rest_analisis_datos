var util_controller = require('./util_controller');
var dbgestion = require('../model/microsoftSQL/dbgestion');

exports.obtener_ruta_ejecutivo_por_dia = function (req, res, next) {
    let dia = req.query.dia?req.query.dia:0;
    let id_ejecutivo = req.query.id_ejecutivo? req.query.id_ejecutivo:0;
    dbgestion.obtener_ruta_ejecutivo_por_dia(dia, id_ejecutivo).then(function (result) {
        res.json(result);
    }).catch(function (error) {
        res.status(403);
        res.json({"error": "Error al consultar ruta!"})
    });
};
