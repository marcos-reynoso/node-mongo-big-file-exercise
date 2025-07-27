const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const Records = require('./records.model');

const upload = async (req, res) => {
	const { file } = req;
	if (!file) return res.status(400).json({ message: 'No file uploaded' });

	const filePath = path.resolve(file.path);
	const BATCH_SIZE = 1000;
	let batch = [];
	let total = 0;

	try {
		await new Promise((resolve, reject) => {
			const stream = fs
				.createReadStream(filePath)
				.pipe(csv())
				.on('data', async (row) => {
					batch.push({
						id: Number(row.id),
						firstname: row.firstname,
						lastname: row.lastname,
						email: row.email,
						email2: row.email2,
						profession: row.profession,
					});
					if (batch.length >= BATCH_SIZE) {
						stream.pause();
						try {
							await Records.insertMany(batch);
							total += batch.length;
							batch = [];
							stream.resume();
						} catch (err) {
							reject(err);
						}
					}
				})
				.on('end', async () => {
					if (batch.length > 0) {
						await Records.insertMany(batch);
						total += batch.length;
					}
					resolve();
				})
				.on('error', reject);
		});

		fs.unlinkSync(filePath);
		return res
			.status(200)
			.json({
				message: `Archivo procesado correctamente. Registros insertados: ${total}`,
			});
	} catch (err) {
		if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
		return res
			.status(500)
			.json({ message: 'Error procesando el archivo', error: err.message });
	}
};

const list = async (_, res) => {
	try {
		const data = await Records.find({}).limit(10).lean();

		return res.status(200).json(data);
	} catch (err) {
		return res.status(500).json(err);
	}
};

module.exports = {
	upload,
	list,
};
