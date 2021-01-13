let express = require('express');
let router = express.Router();
let trx_controller = require('../controller/trx_controller');
//let comercios_controller = require('../controller/comercios_controller');

router.get('/obtener_trx_n_periodo', trx_controller.obtener_trx_n_periodo);
router.get('/obtener_comercios', trx_controller.obtener_comercios);

module.exports = router;