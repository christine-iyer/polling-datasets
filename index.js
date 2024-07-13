const fs = require('fs');
const path = require('path');
const Papa = require('papaparse');
const { parse: json2csv } = require('json2csv');

const csvFiles = [
    'congressApproval.csv', 
    'approvalAverages.csv', 
    'favorabilityAverages.csv', 
    'favorabilityPolls.csv', 
    'genericBallotAverages.csv', 
    'genericBallotPollsHistorical.csv', 
    'genericBallotPolls.csv', 
    'housePollsHistorical.csv', 
    'presidentApprovalPollsHistorical.csv', 
    'presidentApprovalPolls.csv',
    'presidentPollsHistorical.csv', 
    'presidentPolls.csv', 
    'presidentialGeneralAverages.csv', 
    'senatePollsHistorical.csv', 
    'senatePolls.csv', 
    'vpApprovalPolls.csv'
];

const mergedData = [];
let allColumns = new Set(['poll_name']); // Initialize with 'poll_name' column

function readCsvFile(filePath) {
    return new Promise((resolve, reject) => {
        fs.readFile(filePath, 'utf8', (err, fileContent) => {
            if (err) {
                return reject(err);
            }
            Papa.parse(fileContent, {
                header: true,
                complete: (results) => resolve(results.data),
                error: (err) => reject(err)
            });
        });
    });
}

async function mergeCsvFiles(files) {
    try {
        for (const file of files) {
            console.log(`Processing file: ${file}`);
            const data = await readCsvFile(file);
            const pollName = path.basename(file, '.csv');
            data.forEach(row => {
                row.poll_name = pollName; // Add poll_name column
                mergedData.push(row);
                Object.keys(row).forEach(key => allColumns.add(key));
            });
        }
        
        // Convert Set to Array
        allColumns = Array.from(allColumns);
        
        // Create a new array with merged data
        const mergedDataWithAllColumns = mergedData.map(row => {
            const completeRow = {};
            allColumns.forEach(column => {
                completeRow[column] = row[column] || '';
            });
            return completeRow;
        });
        
        // Convert JSON to CSV
        const csv = json2csv(mergedDataWithAllColumns, { fields: allColumns });
        
        // Write the merged CSV to a file
        fs.writeFileSync('merged.csv', csv);
        console.log('Merged CSV file created successfully as merged.csv');
    } catch (err) {
        console.error('Error during merging CSV files:', err);
    }
}

mergeCsvFiles(csvFiles);
