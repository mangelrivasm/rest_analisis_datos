const csv = require('csv-parser');
const fs = require('fs');

fs.createReadStream('C:/Users/USUARIO/Documents/ESTUDIOS/TESIS/data/TB_TRANSACTION_LOG.csv')
  .pipe(csv())
  .on('data', (row) => {
    console.log(row);
  })
  .on('end', () => {
    console.log('CSV file successfully processed');
  });