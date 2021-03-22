require('dotenv').config()

const { Requester, Validator } = require('@chainlink/external-adapter')
const moment = require('moment')
const _ = require("lodash")


const customError = (data) => {
  if (data.Response === 'Error') return true
  return false
}

const KelvinToFahrenHeit = (kelvin) => {
  return ((kelvin-273.15)*1.8)+32;
}

const customParams = {
  city_name: true,
  end: true,
  days: true, 
  threshold: false
}

const createRequest = (input, callback) => {
  const validator = new Validator(input, customParams)
  if (validator.error) return callback(validator.error.statusCode, validator.error)
  const jobRunID = validator.validated.id
  const city_name = validator.validated.data.city_name
  const end = validator.validated.data.end
  const days = validator.validated.data.days
  const appid = process.env.APP_ID

  // api has 7 day limit so we need to create several requests
  let daySeconds = 86400
  let start = end - (days * daySeconds)
  let endRequest = 0;
  let requests = []
  

  const buildRequests = (daysLeft, start) => {
      if(daysLeft >= 7) endRequest = start + (7 * daySeconds)
      else endRequest = start + (daysLeft * daySeconds)
      requests.push({start: start, end: endRequest, days: ((endRequest - start) / 86400), url: `http://history.openweathermap.org/data/2.5/history/city?q=${city_name}&type=hour&start=${start}&end=${end}&appid=${appid}`})
      daysLeft = daysLeft - 7
      start = endRequest;
      if(daysLeft > 0) buildRequests(daysLeft, start)
  }
  buildRequests(days, start)

  // create request array
  let promiseArray = []
  requests.forEach(req => {
    promiseArray.push(Requester.request({url: req.url}, customError))
  })

  // when response is in, clean up array and convert timestamp to dates
  Promise.all(promiseArray).then(response => {
    let joinedList = []
    response.forEach((response) => {
      response.data.list.forEach((item) => {
        joinedList.push({
          dt: item.dt,
          temp_min: item.main.temp_min,
          temp_max: item.main.temp_max,
          day_utc: moment.unix(item.dt).format("MM/DD/YYYY")
        })
      })
    })

    // group items by day to calculate average and HDD
    let listByDate = _.chain(joinedList)
    .groupBy("day_utc")
    .map((value, key) => ({ day_utc: key, items: value }))
    .value()

    listByDate = listByDate.map(date => {
      let min = Math.min.apply(Math, date.items.map(function(o) { return o.temp_min; }))
      let max = Math.max.apply(Math, date.items.map(function(o) { return o.temp_max; }))
      let avg = (min + max) / 2
      return {
        date: date.day_utc,
        min: KelvinToFahrenHeit(min),
        max: KelvinToFahrenHeit(max),
        avg: (KelvinToFahrenHeit(avg)),
        hdd: Math.max(0, 65 - parseInt(KelvinToFahrenHeit(avg))),
        cdd: Math.max(0, parseInt(KelvinToFahrenHeit(avg)) - 65)
      }
    })
    
    const reducer = (accumulator, currentValue) => (accumulator) + (currentValue);
    let totalHDD = listByDate.map(item => item.hdd).reduce(reducer)
    let totalCDD = listByDate.map(item => item.cdd).reduce(reducer)

    let resp = {
      data: {
        hdd: totalHDD,
        cdd: totalCDD
      },
    }
    callback(200, Requester.success(jobRunID, resp))

  }).catch(error => {

    callback(500, Requester.errored(jobRunID, error))
  })
}

// This is a wrapper to allow the function to work with
// GCP Functions
exports.gcpservice = (req, res) => {
  createRequest(req.body, (statusCode, data) => {
    res.status(statusCode).send(data)
  })
}

// This is a wrapper to allow the function to work with
// AWS Lambda
exports.handler = (event, context, callback) => {
  createRequest(event, (statusCode, data) => {
    console.log(event)
    callback(null, data)
  })
}

// This is a wrapper to allow the function to work with
// newer AWS Lambda implementations
exports.handlerv2 = (event, context, callback) => {
  console.log(event)
  createRequest(JSON.parse(event.body), (statusCode, data) => {
    callback(null, {
      statusCode: statusCode,
      body: JSON.stringify(data),
      isBase64Encoded: false
    })
  })
}

module.exports.createRequest = createRequest
