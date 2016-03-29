var db = require('alasql')

module.exports.loadCsv = function(db, path, callback) {
	db("CREATE TABLE IF NOT EXISTS APICALLS(RequestedIntegratorKey VARCHAR, ApiAction VARCHAR, ApiUri VARCHAR, ApiStartTime VARCHAR, ApiEnvelopeId VARCHAR, ApiStatus VARCHAR, ApiSource VARCHAR, ApiException VARCHAR, ApiWho VARCHAR, ApiAccountName VARCHAR, AccountId VARCHAR, ApiIPAddress VARCHAR)");
	db("SELECT * FROM CSV('" + path + "',{separator:'\t',headers:false})", [], function(apiCallsData) {
		if (callback) {
			db.tables.APICALLS = {
				data: apiCallsData,
			};
			callback(null, apiCallsData);
		}
	});
}

module.exports.allRows = function(db, callback) {
	db("SELECT * FROM APICALLS", [], function(data) {
		if (callback) {
			callback(null, data);
		}
	});
}

module.exports.checkApiCalls = function(db, callback) {
	db("SELECT COUNT(*) as total FROM APICALLS", [], function(data) {
		if (callback) {
			callback(null, data[0].total);
		}
	});
}

module.exports.checkApiNoErrors = function(db, numberOfCalls, callback) {
	db("SELECT COUNT(*) as total FROM APICALLS WHERE [1] != 'GetLoginInformation' AND [7] != 'NULL'", [], function(data) {
		if (callback) {
			callback(null, ((data[0].total * 10) < numberOfCalls));
		}
	});
}

module.exports.getApiErrors = function(db, callback) {
	db("SELECT [1] as ApiAction, [3] as ApiStartTime, [7] as ApiException, [9] as ApiAccountName, [10] as AccountId FROM APICALLS WHERE [1] != 'GetLoginInformation' AND [7] != 'NULL'", [], function(data) {
		if (callback) {
			callback(null, data);
		}
	});
}

module.exports.checkForPolling = function(db, callback) {
	db("SELECT * FROM APICALLS WHERE [1] = 'GetEnvelope' AND [3] != 'NULL' ORDER BY [4], [3] ", [], function(data) {
		var previousDate = new Date(0);
		var previousEnvId = '';
		var fifteenMinutes = 15 * (60 * 1000);

		if (callback) {
			for (var i = 0; i < data.length; i++) {
				var dateString = data[i]['3'];
				var apiStartTime = new Date(dateString);
				var apiEnvelopeId = data[i]['4'];
				var difference = apiStartTime.getTime() - previousDate.getTime();
				if ((apiEnvelopeId === previousEnvId) && (difference < fifteenMinutes)) {
					callback(null, false);
					return;
				}
				previousDate = apiStartTime;
				previousEnvId = apiEnvelopeId;
			}

			callback(null, true);
		}
	});
}

module.exports.checkForPolling2 = function(db, callback) {
	db("SELECT * FROM APICALLS WHERE [1] = 'GetDocument' AND [3] != 'NULL' ORDER BY [4], [3] ", [], function(data) {
		var previousDate = new Date(0);
		var previousEnvId = '';
		var previousDocId = '';
		var fifteenMinutes = 15 * (60 * 1000);

		if (callback) {
			for (var i = 0; i < data.length; i++) {
				var dateString = data[i]['3'];
				var apiStartTime = new Date(dateString);
				var apiEnvelopeId = data[i]['4'];
				//extract the document ID from the endpoint
				var n = data[i]['2'].lastIndexOf('/');
				var apiDocId = data[i]['2'].substring(n+1, data[i]['2'].length);
				var difference = apiStartTime.getTime() - previousDate.getTime();
				if ((apiEnvelopeId === previousEnvId) && (apiDocId === previousDocId) && (difference < fifteenMinutes)) {
					callback(null, false);
					return;
				}
				previousDate = apiStartTime;
				previousEnvId = apiEnvelopeId;
				previousDocId = apiDocId;
			}

			callback(null, true);
		}
	});
}

module.exports.report = function(username, path, callback) {
	var analyzer = this;
	analyzer.loadCsv(db, path, function(err, data) {
		if (err || !data) {
			throw new Error("Cannot load results!");
		}
		analyzer.checkApiCalls(db, function(err, data) {
			if (err || !data) {
				throw new Error("Cannot get API calls count!");
			}
			var numberOfCalls = data;
			analyzer.checkApiNoErrors(db, numberOfCalls, function(err, data) {
				if (err) {
					throw new Error("Cannot check if API calls have errors!");
				}
				var errorCheck = data;
				analyzer.getApiErrors(db, function(err, data) {
					if (err || !data) {
						throw new Error("Cannot get API calls erros!");
					}
					var allErrors = data;
					analyzer.checkForPolling(db, function(err, data) {
						if (err) {
							throw new Error("Cannot check for polling GetEnvelope!");
						}
						var pollingCheck1 = data;
						analyzer.checkForPolling2(db, function(err, data) {
							if (err) {
								throw new Error("Cannot check for polling GetDocument!");
							}
							var pollingCheck = (pollingCheck1 && data);

							var passFail = (numberOfCalls > 0) && errorCheck && pollingCheck;
							var results = "\n- Application meets minimum criteria: " + passFail;

							results += "\n- Was the app tested against demo: " + (numberOfCalls > 20) + ", number of calls: " + (numberOfCalls);
							results += "\n- Acceptable level of errors: " + (errorCheck);
							results += "\n- No excessive polling detected: " + (pollingCheck);
							if (!errorCheck) {
								var apiAction, apiStartTime, apiErrorCode, apiAccountName, errorAccountId;
								for (var i = 0; i < allErrors.length; i++) {
									apiAction = allErrors[i]["ApiAction"];
									apiStartTime = allErrors[i]["ApiStartTime"];
									apiErrorCode = allErrors[i]["ApiException"];
									apiAccountName = allErrors[i]["ApiAccountName"];
									errorAccountId = allErrors[i]["AccountId"];
									results += "\n- ERROR!! API call: " + apiAction + ", exception: " + apiErrorCode + ", time: " + apiStartTime + ", accountName: " + apiAccountName + ", account id: " + errorAccountId;
								}
							}
							results += "\nReport generated by user: " + username + " on " + new Date();
							if (callback) {
								callback(null, results);
							}
						});
					});
				});
			});
		});
	});

}