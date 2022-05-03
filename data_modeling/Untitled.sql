-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema IMDB
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema IMDB
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `IMDB` DEFAULT CHARACTER SET utf8 ;
USE `IMDB` ;

-- -----------------------------------------------------
-- Table `IMDB`.`movie`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `IMDB`.`movie` ;

CREATE TABLE IF NOT EXISTS `IMDB`.`movie` (
  `mid` INT NOT NULL AUTO_INCREMENT,
  `title` VARCHAR(45) NULL,
  `price` DECIMAL(2) NULL,
  PRIMARY KEY (`mid`),
  INDEX `idx_title` (`title` ASC))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `IMDB`.`member`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `IMDB`.`member` ;

CREATE TABLE IF NOT EXISTS `IMDB`.`member` (
  `member_id` INT NOT NULL AUTO_INCREMENT,
  `member_name` VARCHAR(45) NULL,
  PRIMARY KEY (`member_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `IMDB`.`review`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `IMDB`.`review` ;

CREATE TABLE IF NOT EXISTS `IMDB`.`review` (
  `review_id` INT NOT NULL AUTO_INCREMENT,
  `review` VARCHAR(45) NULL,
  `rating` INT NULL,
  `mid` INT NOT NULL,
  `member_id` INT NOT NULL,
  PRIMARY KEY (`review_id`),
  INDEX `fk_review_movie1_idx` (`mid` ASC),
  INDEX `fk_review_member1_idx` (`member_id` ASC),
  INDEX `idx_rating` (`rating` ASC),
  CONSTRAINT `fk_review_movie1`
    FOREIGN KEY (`mid`)
    REFERENCES `IMDB`.`movie` (`mid`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_review_member1`
    FOREIGN KEY (`member_id`)
    REFERENCES `IMDB`.`member` (`member_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `IMDB`.`cast`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `IMDB`.`cast` ;

CREATE TABLE IF NOT EXISTS `IMDB`.`cast` (
  `cast_id` INT NOT NULL AUTO_INCREMENT,
  `cast_name` VARCHAR(45) NULL,
  PRIMARY KEY (`cast_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `IMDB`.`movie_guest`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `IMDB`.`movie_guest` ;

CREATE TABLE IF NOT EXISTS `IMDB`.`movie_guest` (
  `mid` INT NOT NULL,
  `member_id` INT NOT NULL,
  `create_ts` DATETIME NULL,
  `quantity` INT NULL,
  PRIMARY KEY (`mid`, `member_id`),
  INDEX `fk_movie_has_member_member1_idx` (`member_id` ASC),
  INDEX `fk_movie_has_member_movie1_idx` (`mid` ASC),
  INDEX `idx_create_ts` (`create_ts` ASC),
  CONSTRAINT `fk_movie_has_member_movie1`
    FOREIGN KEY (`mid`)
    REFERENCES `IMDB`.`movie` (`mid`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_movie_has_member_member1`
    FOREIGN KEY (`member_id`)
    REFERENCES `IMDB`.`member` (`member_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `IMDB`.`movie_actor`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `IMDB`.`movie_actor` ;

CREATE TABLE IF NOT EXISTS `IMDB`.`movie_actor` (
  `mid` INT NOT NULL,
  `cast_id` INT NOT NULL,
  PRIMARY KEY (`mid`, `cast_id`),
  INDEX `fk_movie_has_cast_cast1_idx` (`cast_id` ASC),
  INDEX `fk_movie_has_cast_movie1_idx` (`mid` ASC),
  CONSTRAINT `fk_movie_has_cast_movie1`
    FOREIGN KEY (`mid`)
    REFERENCES `IMDB`.`movie` (`mid`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_movie_has_cast_cast1`
    FOREIGN KEY (`cast_id`)
    REFERENCES `IMDB`.`cast` (`cast_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `IMDB`.`movie_director`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `IMDB`.`movie_director` ;

CREATE TABLE IF NOT EXISTS `IMDB`.`movie_director` (
  `mid` INT NOT NULL,
  `cast_id` INT NOT NULL,
  PRIMARY KEY (`mid`, `cast_id`),
  INDEX `fk_movie_has_cast_cast2_idx` (`cast_id` ASC),
  INDEX `fk_movie_has_cast_movie2_idx` (`mid` ASC),
  CONSTRAINT `fk_movie_has_cast_movie2`
    FOREIGN KEY (`mid`)
    REFERENCES `IMDB`.`movie` (`mid`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_movie_has_cast_cast2`
    FOREIGN KEY (`cast_id`)
    REFERENCES `IMDB`.`cast` (`cast_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

USE `IMDB` ;

-- -----------------------------------------------------
-- Placeholder table for view `IMDB`.`v_movie_rating`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `IMDB`.`v_movie_rating` (`mid` INT, `title` INT, `avg(rating)` INT, `sum(g.quantity) * m.price` INT);

-- -----------------------------------------------------
-- View `IMDB`.`v_movie_rating`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `IMDB`.`v_movie_rating`;
DROP VIEW IF EXISTS `IMDB`.`v_movie_rating` ;
USE `IMDB`;
CREATE  OR REPLACE VIEW `v_movie_rating` AS
select m.mid, m.title, avg(rating), sum(g.quantity) * m.price
from movie m inner join review r on m.mid = r.mid
inner join movie_guest g on m.mid = g.mid
group by m.mid, m.title
order by m.mid, m.title;

SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
