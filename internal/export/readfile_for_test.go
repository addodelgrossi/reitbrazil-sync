package export_test

import "os"

func osReadFileOrSkip(path string) ([]byte, error) { return os.ReadFile(path) }
