const { MongoClient } = require('mongodb')
const fs = require('fs')
const { join } = require('path')
const csvparse = require('csv-parse')
const { parse } = require('date-fns')
const countryMap = require('./countrymap')

require('dotenv').config()

const dbUrl = process.env.MONGODB_URI

const readData = file => new Promise((resolve, reject) => {
	const output = []
	const csvparser = csvparse({
		columns: true,
		delimiter: ','
	})
	csvparser.on('readable', () => {
		let record = csvparser.read()
		while (record) {
			output.push(record)
			record = csvparser.read()
		}
	})
	csvparser.on('error', (err) => {
		reject(err)
	})
	csvparser.on('end', () => {
		resolve(output)
	})
	const filename = join(__dirname, file)
	const readStream = fs.createReadStream(filename)
	readStream.pipe(csvparser)
})

const mapCsvRecord = csvRecord => Object.entries(csvRecord).reduce((acc, [key, value], index) => {
	if (index > 3) {
		const date = parse(key, 'M/d/yy', new Date())
		const continent = countryMap[csvRecord['Country/Region']] || 'Others'
		acc.push({
			continent,
			country: csvRecord['Country/Region'],
			state: csvRecord['Province/State'],
			location: {
				type: 'Point',
				coordinates: [parseFloat(csvRecord.Long), parseFloat(csvRecord.Lat)]
			},
			date,
			count: parseInt(value, 10),
			isCurrent: (index === Object.entries(csvRecord).length - 1)
		})
	}
	return acc
}, [])

const uploadData = async () => {
	let client
	let db
	if (dbUrl) {
		client = await MongoClient.connect(dbUrl, { useNewUrlParser: true, useUnifiedTopology: true })
		db = client.db()
	}
	const csvRecordsConfirmed = await readData('./COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv')
	const recordsConfirmed = csvRecordsConfirmed.flatMap(mapCsvRecord)
	const csvRecordsDeaths = await readData('./COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv')
	const recordsDeaths = csvRecordsDeaths.flatMap(mapCsvRecord)
	const csvRecordsRecovered = await readData('./COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv')
	const recordsRecovered = csvRecordsRecovered.flatMap(mapCsvRecord)

	const recordsMap = new Map()
	recordsConfirmed.forEach((record) => {
		const { count, ...rest } = record
		const key = `${record.location.coordinates[0]},${record.location.coordinates[1]}${record.date}`
		let updatedRecord = recordsMap.get(key)
		if (!updatedRecord) {
			updatedRecord = rest
		}
		updatedRecord.countConfirmed = count
		recordsMap.set(key, updatedRecord)
	})
	recordsDeaths.forEach((record) => {
		const { count, ...rest } = record
		const key = `${record.location.coordinates[0]},${record.location.coordinates[1]}${record.date}`
		let updatedRecord = recordsMap.get(key)
		if (!updatedRecord) {
			updatedRecord = rest
		}
		updatedRecord.countDeaths = count
		recordsMap.set(key, updatedRecord)
	})
	recordsRecovered.forEach((record) => {
		const { count, ...rest } = record
		const key = `${record.location.coordinates[0]},${record.location.coordinates[1]}${record.date}`
		let updatedRecord = recordsMap.get(key)
		if (!updatedRecord) {
			updatedRecord = rest
		}
		updatedRecord.countRecovered = count
		recordsMap.set(key, updatedRecord)
	})
	const records = [...recordsMap.values()]
	const recordsWithCountSick = records.map((record) => {
		const countConfirmed = record.countConfirmed || 0
		const countDeaths = record.countDeaths || 0
		const countRecovered = record.countRecovered || 0
		const countSick = countConfirmed - countDeaths - countRecovered
		return {
			...record,
			countSick
		}
	})

	await db.collection('cases').deleteMany({})
	await db.collection('cases').insertMany(recordsWithCountSick)

	if (client) {
		await client.close()
	}
}

uploadData()
