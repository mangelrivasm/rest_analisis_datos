let express = require('express');
let router = express.Router();
let catalogos_controller = require('../controller/catalogos_controller');

router.post('/obtener_catalogo', catalogos_controller.obtener_catalogo);

module.exports = router;