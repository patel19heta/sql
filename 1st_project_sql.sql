-- data cleaning project

select *
from layoffs;

-- 1. remove duplicates if any
-- 2. standarize the data
-- 3. null values or blank values
-- remove any rows or colums that are unncesaary

create table layoffs_staging
like layoffs;

select * 
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

select *,
row_number() over(partition by company, industry,total_laid_off,percentage_laid_off,`date`) as row_num
from layoffs_staging;

-- select *, to align diff id number
-- row_number() over() as row_num
-- from layoffs_staging;

WITH duplicate_cte as 
(
select *,
row_number() over(partition by company, location,  industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte 
where row_num > 1;

select *
from layoffs_staging
where company = 'casper'; 

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(partition by company, location,  industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging;

-- to delete
select *
from layoffs_staging2 
where row_num > 1;
;
DELETE FROM layoffs_staging2 WHERE row_num > 1;

-- standarizing data

select company, trim(company) 
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry 
from layoffs_staging2
;

select *
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%';

-- to standarize location

update layoffs_staging2
set country =  trim(trailing '.' from country)
where country like 'united states%';

select *
from layoffs_staging2;

-- change date in y/m/d formet

UPDATE layoffs_staging2
SET `date` = CAST(`date` AS DATE);
select *
from layoffs_staging2;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2 where company='airbnb';

select * from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company= t2.company
where (t1.industry is null OR t1.industry='')
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company= t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select *
from layoffs_staging2;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;