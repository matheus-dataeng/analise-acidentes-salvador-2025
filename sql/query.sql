SELECT 
	loc."Id_localizacao" AS Id,
	loc."UF",
	loc."Municipio",
	temp."Data",
	temp."Horario",
	temp."Visibilidade",
	tip."Tipo_acidente",
	tip."Causa_acidente",
	meteo."Condicao_meteorologica",
	meteo."Probabilidade_acidente",
	via."Pista",
	aci."Pessoas",
	aci."Veiculos",
	aci."Feridos",
	aci."Mortos"
FROM acidentes AS aci
LEFT JOIN localizacao AS loc
	ON aci."Id_acidente" = loc."Id_acidente"
LEFT JOIN tempo AS temp
	ON aci."Id_acidente" = temp."Id_acidente"
LEFT JOIN tempo_meteorologico AS meteo
	ON aci."Id_acidente" = meteo."Id_acidente"
LEFT JOIN tipo_acidente as tip
	ON aci."Id_acidente" = tip."Id_acidente"  
LEFT JOIN  via
	ON aci."Id_acidente" = via."Id_acidente"
WHERE loc."UF" = 'BA' AND loc."Municipio" = 'Salvador'
ORDER BY temp."Data" ASC

