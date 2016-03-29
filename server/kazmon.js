var unirest = require('unirest');

const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const kazmonUrl = "https://137.135.43.118:17010/v1/hadoop/jobs";

module.exports.createJob = function(startDate, endDate, integratorKey, companyName, callback) {
  var date = new Date();

	var job;
	var params = {
		"JobType": "Hive",
		"EventQuery": {
			"BeginTime": new Date(Date.parse(startDate)).toISOString(),
			"EndTime": new Date(Date.parse(endDate)).toISOString(),
			//"SelectList": "$.Data.DataPoints.RequestedIntegratorKey, $.Data.DataPoints.ApiAction, $.Data.DataPoints.ApiUri, $.Data.DataPoints.ApiStartTime, $.Data.DataPoints.ApiEnvelopeId, $.Data.DataPoints.ApiErrorCode, $.Data.DataPoints.ApiWho,$.Data.DataPoints.ApiAccountName, $.Data.DataPoints.AccountId",
			"SelectList": "$.Data.DataPoints.RequestedIntegratorKey, $.Data.DataPoints.ApiAction, $.Data.DataPoints.ApiUri, $.Data.DataPoints.ApiStartTime, $.Data.DataPoints.ApiEnvelopeId, $.Data.DataPoints.ApiStatus, $.Data.DataPoints.ApiSource, $.Data.DataPoints.ApiException, $.Data.DataPoints.ApiWho, $.Data.DataPoints.ApiAccountName, $.Data.DataPoints.AccountId, $.Data.DataPoints.ApiIPAddress",
			"WhereClause": "$.Data.DataPoints.RequestedIntegratorKey = '" + integratorKey + "'"
		},
		"JobName": "DS Certifications Node - " + companyName + " - " + months[date.getMonth()] + " " + date.getDate() + " - AAH",
		"JobOwner": "ApiCertifications"
	};
	var request = unirest.post(kazmonUrl);
	//request.options.timeout = 10000;
	request.headers({
			'Accept': 'application/json',
			'Content-Type': 'application/json'
		})
		.strictSSL(false)
		//.timeout(10000)
		.send(params)
		.end(function(response) {
			if (200 === response.statusCode) {
				if (callback) {
					job = response && response.body;
					callback(null, job);
				}
			} else {
				callback(response, null);
			}
		});
}

module.exports.getJobStatus = function(jobId, callback) {
	var request = unirest.get(kazmonUrl + "/" + jobId);
	request.options.rejectUnauthorized = false;
	//request.options.timeout = 10000;
	request.type('json')
		//.timeout(10000)
		.end(function(response) {
			if (200 === response.statusCode) {
				if (callback) {
					job = response && response.body;
					callback(null, job);
				}
			} else {
				callback(response, null);
			}
		});
}

module.exports.getJobResults = function(jobId, callback) {
	var request = unirest.get(kazmonUrl + "/" + jobId + "/result");
	request.options.rejectUnauthorized = false;
	//request.options.timeout = 10000;
	//request.timeout(10000)
		request.end(function(response) {
		if (200 === response.statusCode) {
			if (callback) {
				var results = response && response.body;
				callback(null, results);
			}
		} else {
			callback(response, null);
		}
	});
}