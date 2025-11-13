package gen

const (
	insMain = `
INSERT INTO ln_genBotsMainStats
(botName, botStats, botStatsProp, isNumeric, month, year, project_id)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  botStats = VALUES(botStats),
  botStatsProp = VALUES(botStatsProp),
  isNumeric = VALUES(isNumeric)`

	insBySource = `
INSERT INTO ln_genBotsMainStatsBySource
(source, value, valueProp, month, year, project_id)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  value = VALUES(value),
  valueProp = VALUES(valueProp)`

	insByMethod = `
INSERT INTO ln_genBotsMainStatsByMethod
(method, value, valueProp, month, year, project_id)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  value = VALUES(value),
  valueProp = VALUES(valueProp)`

	insByVerification = `
INSERT INTO ln_genBotsMainStatsByVerification
(verified, unverified, month, year, project_id)
VALUES (?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  verified = VALUES(verified),
  unverified = VALUES(unverified)
`

	insByRefPage = `
INSERT INTO ln_genBotsMainStatsByRefPage
(url, value, valueProp, month, year, project_id)
VALUES (?, ?, ?, ?, ?, ?)
`

	insByTarget = `
INSERT INTO ln_genBotsMainStatsByTarget
(target, value, valueProp, month, year, project_id)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  value     = VALUES(value),
  valueProp = VALUES(valueProp)`

	insByProtVersion = `
INSERT INTO ln_genBotsMainStatsByProtVersion
(protocol, value, valueProp, month, year, project_id)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  value     = VALUES(value),
  valueProp = VALUES(valueProp)`

	insBySitemap = `
INSERT INTO ln_genBotsMainStatsBySitemap
(url, value, valueProp, month, year, project_id)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  value = VALUES(value),
  valueProp = VALUES(valueProp)`
)
