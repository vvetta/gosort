package domain

import "errors"

var (
	ErrWithKFlag     = errors.New("Usage: -k <column number>")
	ErrKFlagLessZero = errors.New("-k flag must be greater then 0")

	ErrFileNotFound = errors.New("file not found!")
	ErrReadingFile  = errors.New("file reading error!")
)
