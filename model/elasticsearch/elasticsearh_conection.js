let elasticsearch = require('elasticsearch');
let config= require('../../config');
let client = new elasticsearch.Client({
    hosts: config.hosts_elasticsearch,
    log: 'trace'
});

exports.client = client;