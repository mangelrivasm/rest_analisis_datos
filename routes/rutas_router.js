let express = require('express');
let router = express.Router();
let util_autenticacion = require('../util_autenticacion');
let rutas_controller = require('../controller/rutas_controller');


router.get('/obtener_ruta_ejecutivo_por_dia', function (req, res, next) {
    rutas_controller.obtener_ruta_ejecutivo_por_dia(req, res, next);
});


module.exports = router;