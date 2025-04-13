const ExcelJS = require('exceljs');
const fs = require('fs');

async function analyze(orders) {
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Orders Analysis');

    worksheet.columns = [
        { header: 'Order ID', key: 'id_order', width: 15 },
        { header: 'Address', key: 'address', width: 30 },
        { header: 'Work Stages', key: 'work_stages', width: 30 },
        { header: 'Work Prices', key: 'work_prices', width: 20 },
        { header: 'Materials', key: 'materials', width: 30 },
        { header: 'Material Quantities', key: 'material_quantities', width: 20 },
        { header: 'Material Prices', key: 'material_prices', width: 20 }
    ];

    orders.forEach(order => {
        worksheet.addRow(order);
    });

    const filePath = './output/orders_analysis.xlsx';
    await workbook.xlsx.writeFile(filePath);
    console.log(`Excel file saved: ${filePath}`);
}

const ordersJson = process.argv[2];
if (ordersJson) {
    const orders = JSON.parse(ordersJson);
    analyze(orders).catch(err => console.error('Error:', err));
}
