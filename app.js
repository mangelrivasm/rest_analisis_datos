var express = require('express');
var path = require('path');
var session = require('express-session');
var sessionstore = require('sessionstore');
var util_autenticacion = require('./util_autenticacion');
//var favicon = require('serve-favicon');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var config = require('./config');
var index_router = require('./routes/index_router');
var catalogos_router = require('./routes/catalogos_router');
var ventas_router = require('./routes/ventas_router');
var rutas_router = require('./routes/rutas_router');
var trx_router = require('./routes/trx_router');


var app = express();

var sessionMiddleware = session({
    secret: config.secret,
    store: sessionstore.createSessionStore({
        type: 'elasticsearch',
        host: config.host_elasticsearch,
        prefix: '',
        index: 'session',
        typeName: 'session',
        pingInterval: 1000,
        timeout: 10*60
    }),
    saveUninitialized: false,
    resave: false
});
app.use(sessionMiddleware);

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'hbs');

// uncomment after placing your favicon in /public
//app.use(favicon(path.join(__dirname, 'public', 'favicon.ico')));
app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

//app.use(util_autenticacion.authenticationMiddleware);
app.use('/',index_router);
app.use('/catalogos', catalogos_router);
app.use('/ventas', ventas_router);
app.use('/trx', trx_router);
app.use('/rutas', rutas_router);

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  let err = new Error('No encontrado');
  err.status = 404;
  next(err);
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.json({'error':err.message});
});

module.exports = app;
