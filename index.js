/* eslint-disable no-console */
/* eslint-disable no-restricted-syntax */
/* eslint-disable no-await-in-loop */
const { MongoClient } = require('mongodb')
const fs = require('fs')
const { join } = require('path')
const csvparse = require('csv-parse')
const { addHours, parse } = require('date-fns')
const fetch = require('node-fetch')
const continentData = require('./continentData')

require('dotenv').config()

const dbUrl = process.env.MONGODB_URI
const googleApiKey = process.env.GOOGLE_API_KEY

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
		const date = addHours(parse(key, 'M/d/yy', new Date()), 2)
		let lng = parseFloat(csvRecord.Long)
		let lat = parseFloat(csvRecord.Lat)
		// Fix for 0,0 coordinates
		if (lng === 0 && lat === 0) {
			lng = -106.3467712
			lat = 56.1303673
		}
		acc.push({
			country: csvRecord['Country/Region'],
			location: {
				type: 'Point',
				coordinates: [lng, lat]
			},
			date,
			count: parseInt(value, 10),
			isCurrent: (index === Object.entries(csvRecord).length - 1)
		})
	}
	return acc
}, [])

const uploadData = async () => {
	// Read records from CSV files and merge them
	const csvRecordsConfirmed = await readData('./COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')
	const recordsConfirmed = csvRecordsConfirmed.flatMap(mapCsvRecord)
	const csvRecordsDeaths = await readData('./COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')
	const recordsDeaths = csvRecordsDeaths.flatMap(mapCsvRecord)
	const recordsMap = new Map()
	recordsConfirmed.forEach((record) => {
		const { count, ...rest } = record
		const key = `${record.location.coordinates[0]}|${record.location.coordinates[1]}|${record.date}`
		let updatedRecord = recordsMap.get(key)
		if (!updatedRecord) {
			updatedRecord = rest
		}
		updatedRecord.countConfirmed = count
		recordsMap.set(key, updatedRecord)
	})
	recordsDeaths.forEach((record) => {
		const { count, ...rest } = record
		const key = `${record.location.coordinates[0]}|${record.location.coordinates[1]}|${record.date}`
		let updatedRecord = recordsMap.get(key)
		if (!updatedRecord) {
			updatedRecord = rest
		}
		updatedRecord.countDeaths = count
		recordsMap.set(key, updatedRecord)
	})
	const records = [...recordsMap.values()]

	// Collect unique coordinates and query Google for location data
	const locationDataJson = fs.readFileSync(join(__dirname, 'locationData.json'))
	const savedLocationData = JSON.parse(locationDataJson)
	const newCoordinatesMap = records.reduce((acc, record) => {
		const key = `${record.location.coordinates[0]}|${record.location.coordinates[1]}`
		if (!savedLocationData[key]) { // Ignore keys that were already fetched
			acc.set(key, {
				lng: record.location.coordinates[0],
				lat: record.location.coordinates[1]
			})
		}
		return acc
	}, new Map())
	const coordinates = [...newCoordinatesMap.values()]
	const newLocationDataMap = new Map()
	for (const coordinate of coordinates) {
		const key = `${coordinate.lng}|${coordinate.lat}`
		const res = await fetch(`https://maps.googleapis.com/maps/api/geocode/json?key=${googleApiKey}&latlng=${coordinate.lat},${coordinate.lng}&result_type=country`)
		const json = await res.json()
		try {
			const countryShortName = json.results[0].address_components[0].short_name
			newLocationDataMap.set(key, countryShortName)
		} catch (err) {
			console.warn('Could not geocode coordinate', coordinate, json)
		}
	}
	const newLocationData = Object.fromEntries(newLocationDataMap)
	const updatedLocationData = {
		...savedLocationData,
		...newLocationData
	}
	fs.writeFileSync(join(__dirname, 'locationData.json'), JSON.stringify(updatedLocationData, null, 2))

	// Add calculated props to records
	const recordsWithCalculatedProps = records.map((record) => {
		// Continent (treat China as a separate continent)
		const locationKey = `${record.location.coordinates[0]}|${record.location.coordinates[1]}`
		const countryShortName = updatedLocationData[locationKey]
		const continent = (countryShortName === 'CN') ? 'China' : continentData[countryShortName] || 'Others'
		return {
			...record,
			continent
		}
	})

	// Save records in MongoDB
	if (dbUrl) {
		const client = await MongoClient.connect(dbUrl, { useNewUrlParser: true, useUnifiedTopology: true })
		const db = client.db()
		await db.collection('cases').deleteMany({})
		await db.collection('cases').insertMany(recordsWithCalculatedProps)
		await client.close()
	}
}

uploadData()
