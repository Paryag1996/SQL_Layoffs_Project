-- SQl Project - World Layoffs

select * from layoffs;

-- a) Copying the Data to a New Table so as to keep the raw file intact 
Create Table `layoffs_staging` (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_ci;

select * from layoffs_staging;

Insert into layoffs_staging
Select *, ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,
funds_raised_millions) as row_num
from layoffs;

select * from layoffs_staging;

-- b) Data Cleaning
-- 1. Removing Duplicates
select * from layoffs_staging
where row_num > 1;

delete  from layoffs_staging
where row_num > 1;

select * from layoffs_staging
where row_num > 1;

-- 2. Standardize the Data 

-- 2.a. Trimming Blank Spaces
-- For Company Column
select company, trim(company)
from layoffs_staging;

update layoffs_staging
set company = trim(company);

select company, trim(company)
from layoffs_staging;

-- 2.b. Standardizing Columns for EDA
-- For Industry Column
select distinct industry 
from layoffs_staging
order by industry;

select * from layoffs_staging
where industry like 'Crypto%';

update layoffs_staging 
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct industry 
from layoffs_staging
order by industry;

-- For Location Column
select distinct location
from layoffs_staging
order by location;

update layoffs_staging 
set location = 'Florianopolis'
where location like 'FlorianÃ³polis';

update layoffs_staging 
set location = 'Malmo'
where location like 'MalmÃ¶';

update layoffs_staging 
set location = 'Dusseldorf'
where location like 'DÃ¼sseldorf';

select distinct location
from layoffs_staging
order by location;

-- For Country Column
select distinct country
from layoffs_staging
order by country;

update layoffs_staging 
set country = 'United States'
where country like 'United States%';

select distinct country
from layoffs_staging
order by country;

-- 2.c. Changing DataType of Date Column
update layoffs_staging
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date` from layoffs_staging;

alter table layoffs_staging
Modify Column `date` date;

-- 3. Null Values or Blank Values
-- 3.a. For Industry Column
select distinct industry 
from layoffs_staging
order by industry;

select *
from layoffs_staging
where industry = '' or industry is null;

update layoffs_staging
set industry = null
where industry = '';

select t1.industry, t2.industry from layoffs_staging t1
join layoffs_staging t2 
on t1.company = t2.company
where t1.industry is null
and t2.industry is not null;

update layoffs_staging t1
join layoffs_staging t2 
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

-- 3.b. For Total_Laid_Off and Percentage_Laid_Off Columns
select * from layoffs_staging
where total_laid_off is null
and percentage_laid_off is null;

delete from layoffs_staging
where total_laid_off is null
and percentage_laid_off is null;

-- 4. Remove any Column of No Use
-- For Row_Num Column
ALTER TABLE layoffs_staging
DROP COLUMN row_num;

select * from layoffs_staging;

-- c) Exploratory Data Analysis (EDA)
 
select * from layoffs_staging;

-- Finding Max Layoffs and Percentage Layoffs
select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging;

-- Finding Companies having Percentage Layoffs = 100 %
select * from layoffs_staging
where percentage_laid_off = 1
order by total_laid_off DESC;

-- Finding Company and their respective count of layoffs
select company, sum(total_laid_off) as Layoffs
from layoffs_staging
group by 1
order by 2 DESC;

-- Finding Latest and Earliest Date of Layoffs
select min(`date`), max(`date`) 
from layoffs_staging;

-- Finding Industry and their respective count of layoffs
select industry, sum(total_laid_off) as Layoffs
from layoffs_staging
group by 1
order by 2 DESC;

-- Finding Country and their respective count of layoffs
select country, sum(total_laid_off) as Layoffs
from layoffs_staging
group by 1
order by 2 DESC;

-- Finding Date on which Max Layoffs Occured
select `date`, sum(total_laid_off) as Layoffs
from layoffs_staging
group by 1
order by 2 DESC;

-- Finding the Year in which Maximum Layoffs Occured
select Year(`date`) as Year, sum(total_laid_off) as Layoffs
from layoffs_staging
group by 1
order by 2 DESC;

-- Finding Stage and their respective count of layoffs
select stage, sum(total_laid_off) as Layoffs
from layoffs_staging
group by 1
order by 2 DESC;

-- Finding Running Total based on Month
with cte as (select substring(`date`,1,7) as Month, sum(total_laid_off) as Layoffs
from layoffs_staging
where substring(`date`,1,7) is not null
group by 1
order by 1)
select Month, Layoffs, sum(Layoffs) over (order by Month) as Running_Total
from cte;

-- Finding Top 5 Companies with Layoffs for Different Years
with cte as 
(
select company, Year, Laid_off,
dense_rank() over(partition by Year order by Laid_Off DESC) as Ranking 
from 
(
select company, year(`date`) as Year, sum(total_laid_off) as Laid_Off
from layoffs_staging
where year(`date`) is not null
group by 1,2
) as company_year
group by 1,2
)

select * from cte
where ranking <= 5;