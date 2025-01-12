DELIMITER $$

CREATE TRIGGER set_defaults_on_insert
BEFORE INSERT ON table_master
FOR EACH ROW
BEGIN
    -- Set action to 'i' if it's not provided
    IF NEW.action IS NULL OR NEW.action = '' THEN
        SET NEW.action = 'i';
    END IF;

    -- Set creation_date to today's date if it's not provided
    IF NEW.creation_date IS NULL THEN
        SET NEW.creation_date = CURDATE();
    END IF;

    -- Set version to 1 if it's not provided
    IF NEW.version IS NULL THEN
        SET NEW.version = 1;
    END IF;
END$$

DELIMITER ;