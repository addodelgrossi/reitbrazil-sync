// Package cvm is the source adapter for the CVM (Comissão de Valores
// Mobiliários) open data portal. It downloads the monthly "Informe
// Mensal FII" ZIP, parses the CSV inside, and yields
// model.CVMInformeMensal rows.
//
// Portal URL:
//
//	https://dados.cvm.gov.br/dados/FII/DOC/INF_MENSAL/DADOS/inf_mensal_fii_YYYY.zip
//
// CSVs use ';' as delimiter and UTF-8 encoding since 2021. Older files
// use Latin-1; the parser transcodes on the fly if a non-UTF-8 BOM is
// detected.
package cvm
