package cvm_test

import (
	"testing"

	"github.com/addodelgrossi/reitbrazil-sync/internal/sources/cvm"
)

const sampleTrimestralImovelCSV = "CNPJ_Fundo_Classe;Data_Referencia;Versao;Classe;Nome_Imovel;Endereco;Area;Numero_Unidades;Outras_Caracteristicas_Relevantes;Percentual_Vacancia;Percentual_Inadimplencia;Percentual_Receitas_FII;Percentual_Locado;Percentual_Vendido\n" +
	"11.728.688/0001-47;2026-03-31;1;Imovel para renda;Galpao A;Rua A;1000;1;;3,50;0,20;45,00;96,50;0,00\n"

func TestParsePropertyVacancy_ExtractsPercentualVacancia(t *testing.T) {
	zipBytes := buildZip(t, map[string]string{
		"inf_trimestral_fii_imovel_2026.csv": sampleTrimestralImovelCSV,
	})

	var rows int
	for rec, err := range cvm.ParsePropertyVacancy(t.Context(), zipBytes) {
		if err != nil {
			t.Fatal(err)
		}
		rows++
		if rec.CNPJ != "11728688000147" {
			t.Fatalf("cnpj: %s", rec.CNPJ)
		}
		if rec.VacancyPhysical == nil || *rec.VacancyPhysical != 0.035 {
			t.Fatalf("vacancy: %v", rec.VacancyPhysical)
		}
		if rec.LeasedShare == nil || *rec.LeasedShare != 0.965 {
			t.Fatalf("leased: %v", rec.LeasedShare)
		}
	}
	if rows != 1 {
		t.Fatalf("rows: %d", rows)
	}
}
