-- CREATE STAGING TABLE FROM RAW TABLE 

CREATE TABLE layoff_staging
LIKE layoff;

INSERT layoff_staging
SELECT *
FROM layoff;

-- 1. REMOVE DUPLICATES
-- Raw table doesn't have a primary key. So using different method to find duplicates.

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,stage,`date`,country,funds_raised_millions) AS row_num
FROM layoff_staging;

DELETE  
FROM layoffs_staging2
WHERE row_num > 1;

-- 2. STANDARDIZING DATA

SELECT DISTINCT company from layoffs_staging2
order by 1;

-- Removing whitesapces
UPDATE layoffs_staging2
SET company = trim(company);

SELECT DISTINCT industry from layoffs_staging2
order by 1;

-- Changing inconsistent data
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT location from layoffs_staging2
order by 1;

-- Correcting errors
UPDATE layoffs_staging2
SET location =
CASE
    WHEN location = 'DÃ¼sseldorf' THEN location = 'Düsseldorf'
    WHEN location = 'FlorianÃ³polis' THEN 'Florianópolis'
	WHEN location = 'MalmÃ¶' THEN 'Malmö'
END WHERE location IN('DÃ¼sseldorf','FlorianÃ³polis','MalmÃ¶');

SELECT DISTINCT country from layoffs_staging2
order by 1;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country = 'United States.';

-- Formatting date correctly and changing date column from text data type to date
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. NULL VALUES OR BLANK VALUES

-- Checking for null and blank values
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = "";

UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';

-- Filling null values
UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- 4. REMOVE COLUMNS OR ROWS NOT NEEDED

-- Removing rows that won't be useful in EDA
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Removing column that we added in the start to remove duplicates
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;