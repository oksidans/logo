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
(source, value, valueProp, month, year, project_id)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  value = VALUES(value),
  valueProp = VALUES(valueProp)`

	insByVerification = `
INSERT INTO ln_genBotsMainStatsByVerification
(source, value, valueProp, month, year, project_id)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  value = VALUES(value),
  valueProp = VALUES(valueProp)`

	insByRefPage = `
INSERT INTO ln_genBotsMainStatsByRefPage
(source, value, valueProp, month, year, project_id)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  value = VALUES(value),
  valueProp = VALUES(valueProp)`

	insByTarget = `
INSERT INTO ln_genBotsMainStatsByTarget
(source, value, valueProp, month, year, project_id)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  value = VALUES(value),
  valueProp = VALUES(valueProp)`
)
