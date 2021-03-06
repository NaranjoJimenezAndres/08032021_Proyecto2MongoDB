//1 Se quiere saber el valor maximo de cada jugador en las diferentes ligas que participan en la Champions.

db.equipo.aggregate([
    {
        $match:
            { "europa": true }
    },
    {
        $lookup: {
            "localField": "nombreClub",
            "from": "jugador",
            "foreignField": "equipo",
            "as": "jugadorRend"
        }
    },
    {
        $group: {
            _id: { liga: "$liga" },
            jugadorMasValioso: { $max: "$jugadorRend.valorM€" }
        }
    },
]).pretty()


/*
{ "_id" : { "liga" : "FRA" }, "jugadorMasValioso" : [ 180 ] }
{ "_id" : { "liga" : "ENG" }, "jugadorMasValioso" : [ 80 ] }
{ "_id" : { "liga" : "ESP" }, "jugadorMasValioso" : [ 80 ] }
*/


/*//2 Para el equipo se quiere a un crack (rating>=7). El futbol se mide por goles por lo que se quiere saber quienes son los mejores pistoleros
 y al equipo que pertenecen con su ratio goleador*/

db.rendimiento.aggregate([
    {
        $match: {
            $and: [{ "rating": { $gte: 7.00 } },
            { "goles": { $gt: 0 } }]
        }
    },
    {
        $lookup: {
            "localField": "_id",
            "from": "jugador",
            "foreignField": "Cod",
            "as": "caracteristicas"
        }
    },
    {
        $group: {
            _id: {
                equipo: "$caracteristicas.equipo",
                nombre: "$caracteristicas.nombre"
            },
            goles: { $push: "$goles" },
            tiempo_Partido: { $sum: { $divide: ["$minutosT", "$PJ"] } },
            tasa_Gol: {
                $sum: { $divide: ["$minutosT", "$goles"] }
            }
        }
    },
    {
        $set: {
            promedioGolPartido: {
                $round: [{ $divide: ["$tasa_Gol", "$tiempo_Partido"] }, 2]
            }
        }
    },
    {
        $match: { "promedioGolPartido": { $lte: 5.00 } }
    },
    {
        $project: {
            equipo: "$_id.equipo",
            nombre: "$_id.nombre",
            goles: 1,
            _id: 0,
            tiempo_Partido: 1,
            tasa_Gol: 1,
            promedioGolPartido: 1
        }
    },
    {
        $sort: { promedioGolPartido: 1 }
    }
])
/*
{ "goles" : [ 16 ], "tiempo_Partido" : 75.76190476190476, "tasa_Gol" : 99.4375, "promedioGolPartido" : 1.31, "equipo" : [ "ATM" ], "nombre" : [ "Luis Suarez" ] }
{ "goles" : [ 23 ], "tiempo_Partido" : 68.45454545454545, "tasa_Gol" : 98.21739130434783, "promedioGolPartido" : 1.43, "equipo" : [ "PSG" ], "nombre" : [ "Kylian Mbappe" ] }
{ "goles" : [ 14 ], "tiempo_Partido" : 73.8695652173913, "tasa_Gol" : 121.35714285714286, "promedioGolPartido" : 1.64, "equipo" : [ "LEI" ], "nombre" : [ "Jamie Vardy" ] }
{ "goles" : [ 19 ], "tiempo_Partido" : 49.03125, "tasa_Gol" : 82.57894736842105, "promedioGolPartido" : 1.68, "equipo" : [ "SEV" ], "nombre" : [ "Youssef En-Nesyri" ] }
{ "goles" : [ 6 ], "tiempo_Partido" : 56.46666666666667, "tasa_Gol" : 141.16666666666666, "promedioGolPartido" : 2.5, "equipo" : [ "BCN" ], "nombre" : [ "Ansu Fati" ] }
{ "goles" : [ 6 ], "tiempo_Partido" : 81.85714285714286, "tasa_Gol" : 286.5, "promedioGolPartido" : 3.5, "equipo" : [ "EIB" ], "nombre" : [ "Kike Garcia" ] }
{ "goles" : [ 5 ], "tiempo_Partido" : 97.5, "tasa_Gol" : 390, "promedioGolPartido" : 4, "equipo" : [ "RMD" ], "nombre" : [ "Sergio Ramos" ] }*/




//3 Se quieres saber los jugadores que cuesten menos de 25M (ajuste al presupuesto de la entidad) que tenga la posibilidad de salir del club por
// vencimiento de contrato.

db.jugador.aggregate([
    {
        $match:
        {
            $and: [{ "valorM€": { $lte: 25 } },
            { "finContrato": { $lt: new Date("2022 - 06 - 30") } }]
        }
    },

    {
        $lookup: {
            "localField": "Cod",
            "from": "rendimiento",
            "foreignField": "_id",
            "as": "jugador_Rend"
        }
    },
    {
        $project:
        {
            _id: 0,
            goles: "$jugadorRend.goles",
            valor_en_Mill: "$valorM€",
            nombre: "$nombre",
            posicion: 1
        }
    }
]).pretty()

/*
{ "posicion" : "DEL", "valor_en_Mill" : 15, "nombre" : "Bryan Gil" }
{ "posicion" : "DEL", "valor_en_Mill" : 14, "nombre" : "Sergio Ramos" }
{ "posicion" : "DEF", "valor_en_Mill" : 12, "nombre" : "JC Todibo" }
{ "posicion" : "MC", "valor_en_Mill" : 7.5, "nombre" : "Juan Mata" }
{ "posicion" : "DEL", "valor_en_Mill" : 20, "nombre" : "Gareth Bale" }*/


//4 Se quiere hacer un estudio sobre la inversion que realiza los clubs en diferentes ligas 
// en las diferentes posiciones del campo. Con el fin de analisis de tendencias de mercado o el 
// analisis de rivales europeos.


db.equipo.aggregate([
    {
        $lookup: {
            "localField": "nombreClub",
            "from": "jugador",
            "foreignField": "equipo",
            "as": "plantilla"
        }
    }, {

        $unwind: "$plantilla"

    },
    {
        $group: {
            _id: {
                liga: "$liga",
                posicion: "$plantilla.posicion"
            },
            media: { $avg: "$plantilla.valorM€" },

        }
    },
    {
        $project: {
            liga: "$_id.liga",
            posicion: "$_id.posicion",
            _id: 0,
            Valormedio: { $round: ["$media", 2] },
        }
    },
    {
        $sort: { liga: 1 }
    }, {
        $merge: { into: "mediaDatosPos" }
    }
])

/*
{ "liga" : "ENG", "posicion" : "MC", "Valormedio" : 28.75 }
{ "liga" : "ENG", "posicion" : "DEF", "Valormedio" : 52 }
{ "liga" : "ENG", "posicion" : "DEL", "Valormedio" : 23.67 }
{ "liga" : "ESP", "posicion" : "DEF", "Valormedio" : 37.5 }
{ "liga" : "ESP", "posicion" : "MC", "Valormedio" : 26.25 }
{ "liga" : "ESP", "posicion" : "DEL", "Valormedio" : 25.9 }
{ "liga" : "FRA", "posicion" : "DEF", "Valormedio" : 14.33 }
{ "liga" : "FRA", "posicion" : "MC", "Valormedio" : 28.25 }
{ "liga" : "FRA", "posicion" : "DEL", "Valormedio" : 106 }

*/

//5 Se quiere saber el potencial de los jovenes delanteros de la liga española, por tanto 
// se va a medir su participacion en goles en el equipo al que pertenecen.

db.jugador.aggregate([
    {
        $lookup: {
            "localField": "equipo",
            "from": "equipo",
            "foreignField": "nombreClub",
            "as": "plantilla"
        }
    },
    {

        $unwind: "$plantilla"

    },
    {
        $lookup: {
            "localField": "Cod",
            "from": "rendimiento",
            "foreignField": "_id",
            "as": "descripcion"
        }
    },
    {

        $unwind: "$descripcion"

    },
    {
        $match: {
            $and: [{ "posicion": { $eq: "DEL" } },
            { "edad": { $lte: 25 } },
            { "plantilla.liga": { $eq: "ESP" } }
            ]
        }
    },
    {
        $project: {
            jugador: "$nombre",
            edad: "$edad",
            _id: 0,
            club: "$plantilla.nombreClub",
            goles: "$descripcion.goles",
            golesTotal: "$plantilla.golT"
        }
    },
    {
        $set: {
            porcentaje: {
                $round: [
                    {
                        $multiply: [
                            {
                                $divide: ["$goles", "$golesTotal"]
                            }, 100]
                    },
                    2]
            }
        }
    }
])

/*
{ "jugador" : "Vinicius Jr", "edad" : 20, "club" : "RMD", "goles" : 3, "golesTotal" : 52, "porcentaje" : 5.77 }
{ "jugador" : "Bryan Gil", "edad" : 20, "club" : "EIB", "goles" : 3, "golesTotal" : 30, "porcentaje" : 10 }
{ "jugador" : "Fernando Niño", "edad" : 20, "club" : "VIL", "goles" : 7, "golesTotal" : 57, "porcentaje" : 12.28 }
{ "jugador" : "Ansu Fati", "edad" : 18, "club" : "BCN", "goles" : 6, "golesTotal" : 79, "porcentaje" : 7.59 }
{ "jugador" : "Youssef En-Nesyri", "edad" : 23, "club" : "SEV", "goles" : 19, "golesTotal" : 59, "porcentaje" : 32.2 }*/

//6 Que jugadores acaban contrato en los proximos 5 años, divididos por año.

var arr = [];
["2021-06-30", "2022-06-30", "2023-06-30", "2024-06-30", "2025-06-30"].forEach(function (d) {
    arr.push(new ISODate(d));
});

db.jugador.aggregate([
    {
        $bucket: {
            groupBy: "$finContrato",
            boundaries: arr,
            default: "ProxAgenteLibre",
            output: {
                "count": { $sum: 1 },
                "jugador":
                {
                    $push: {
                        "nombre": "$nombre",
                        "fin_contrato": "$finContrato",
                        "ValorMercado": "$valorM€"
                    }
                }
            }
        }
    }
]).pretty()

/*/*{
        "_id" : "ProxAgenteLibre",
        "count" : 7,
        "jugador" : [
                {
                        "nombre" : "Bryan Gil",
                        "fin_contrato" : ISODate("2021-06-29T22:00:00Z"),
                        "ValorMercado" : 15
                },
                {
                        "nombre" : "Sergio Ramos",
                        "fin_contrato" : ISODate("2021-06-29T22:00:00Z"),
                        "ValorMercado" : 14
                },
                {
                        "nombre" : "Florian Thauvin",
                        "fin_contrato" : ISODate("2021-06-29T22:00:00Z"),
                        "ValorMercado" : 32
                },
                {
                        "nombre" : "JC Todibo",
                        "fin_contrato" : ISODate("2021-06-29T22:00:00Z"),
                        "ValorMercado" : 12
                },
                {
                        "nombre" : "Sergio Aguero",
                        "fin_contrato" : ISODate("2021-06-29T22:00:00Z"),
                        "ValorMercado" : 35
                },
                {
                        "nombre" : "Juan Mata",
                        "fin_contrato" : ISODate("2021-06-29T22:00:00Z"),
                        "ValorMercado" : 7.5
                },
                {
                        "nombre" : "Gareth Bale",
                        "fin_contrato" : ISODate("2021-06-29T22:00:00Z"),
                        "ValorMercado" : 20
                }
        ]
}
{
        "_id" : ISODate("2021-06-30T00:00:00Z"),
        "count" : 5,
        "jugador" : [
                {
                        "nombre" : "Lucas Ocampos",
                        "fin_contrato" : ISODate("2022-06-29T22:00:00Z"),
                        "ValorMercado" : 45
                },
                {
                        "nombre" : "Kylian Mbappe",
                        "fin_contrato" : ISODate("2022-06-29T22:00:00Z"),
                        "ValorMercado" : 180
                },
                {
                        "nombre" : "Cesc Fabregas",
                        "fin_contrato" : ISODate("2022-06-29T22:00:00Z"),
                        "ValorMercado" : 6.5
                },
                {
                        "nombre" : "Hamari Traore",
                        "fin_contrato" : ISODate("2022-06-29T22:00:00Z"),
                        "ValorMercado" : 8
                },
                {
                        "nombre" : "Andreas Christensen",
                        "fin_contrato" : ISODate("2022-06-29T22:00:00Z"),
                        "ValorMercado" : 24
                }
        ]
}
{
        "_id" : ISODate("2022-06-30T00:00:00Z"),
        "count" : 9,
        "jugador" : [
                {
                        "nombre" : "Iñigo Martinez",
                        "fin_contrato" : ISODate("2023-06-29T22:00:00Z"),
                        "ValorMercado" : 25
                },
                {
                        "nombre" : "Kike Garcia",
                        "fin_contrato" : ISODate("2023-06-29T22:00:00Z"),
                        "ValorMercado" : 2
                },
                {
                        "nombre" : "Nabil Fekir",
                        "fin_contrato" : ISODate("2023-06-29T22:00:00Z"),
                        "ValorMercado" : 25
                },
                {
                        "nombre" : "Toni Kroos",
                        "fin_contrato" : ISODate("2023-06-29T22:00:00Z"),
                        "ValorMercado" : 50
                },
                {
                        "nombre" : "Sergio Canales",
                        "fin_contrato" : ISODate("2023-06-29T22:00:00Z"),
                        "ValorMercado" : 20
                },
                {
                        "nombre" : "Denis Suarez",
                        "fin_contrato" : ISODate("2023-06-29T22:00:00Z"),
                        "ValorMercado" : 10
                },
                {
                        "nombre" : "Houssem Aouar",
                        "fin_contrato" : ISODate("2023-06-29T22:00:00Z"),
                        "ValorMercado" : 50
                },
                {
                        "nombre" : "Virgil van Dijk",
                        "fin_contrato" : ISODate("2023-06-29T22:00:00Z"),
                        "ValorMercado" : 80
                },
                {
                        "nombre" : "Jamie Vardy",
                        "fin_contrato" : ISODate("2023-06-29T22:00:00Z"),
                        "ValorMercado" : 16
                }
        ]
}
{
        "_id" : ISODate("2023-06-30T00:00:00Z"),
        "count" : 4,
        "jugador" : [
                {
                        "nombre" : "Jules Kounde",
                        "fin_contrato" : ISODate("2024-06-29T22:00:00Z"),
                        "ValorMercado" : 50
                },
                {
                        "nombre" : "Fernando Niño",
                        "fin_contrato" : ISODate("2024-06-29T22:00:00Z"),
                        "ValorMercado" : 3
                },
                {
                        "nombre" : "Luis Suarez",
                        "fin_contrato" : ISODate("2024-06-29T22:00:00Z"),
                        "ValorMercado" : 15
                },
                {
                        "nombre" : "Yannick Carrasco",
                        "fin_contrato" : ISODate("2024-06-29T22:00:00Z"),
                        "ValorMercado" : 30
                }
        ]
}
{
        "_id" : ISODate("2024-06-30T00:00:00Z"),
        "count" : 5,
        "jugador" : [
                {
                        "nombre" : "Vinicius Jr",
                        "fin_contrato" : ISODate("2025-06-29T22:00:00Z"),
                        "ValorMercado" : 40
                },
                {
                        "nombre" : "Ansu Fati",
                        "fin_contrato" : ISODate("2025-06-29T22:00:00Z"),
                        "ValorMercado" : 80
                },
                {
                        "nombre" : "Youssef En-Nesyri",
                        "fin_contrato" : ISODate("2025-06-29T22:00:00Z"),
                        "ValorMercado" : 15
                },
                {
                        "nombre" : "Sven Botman",
                        "fin_contrato" : ISODate("2025-06-29T22:00:00Z"),
                        "ValorMercado" : 23
                },
                {
                        "nombre" : "Thomas Partey",
                        "fin_contrato" : ISODate("2025-06-29T22:00:00Z"),
                        "ValorMercado" : 50
                }
        ]
}*/

//7 Se quiere analizar los datos desde dos puntos de vista diferente, desde el enfoque del
// valor de mercado y el año de expiracion de contrato.

db.jugador.aggregate([
    {
        $facet: {                               // Escenario superior creado por $facet
            "valor": [                            // campo de salida 1
                {
                    $bucket: {
                        groupBy: "$valorM€",            // Agrupacion de los campos
                        boundaries: [0, 25, 50, 100, 200],  // limites establecidos para los cubos o contenedores
                        default: "Other",             // id establecido para los datos que no estan dentro de los limites anteriores 
                        output: {                     // salida para cada contenedor o cubo
                            "count": { $sum: 1 },
                            "jugador": { $push: { "nombre": "$nombre", "valorM€": "$valorM€" } }, //campos que queremos mantener del estadío anterior
                            "edadMedia": { $avg: "$edad" }         // calculos realizados con los datos
                        }
                    }
                }
            ],
            "añoContrato": [                      // campo de salida 2
                {
                    $bucket: {
                        groupBy: "$finContrato",
                        boundaries: arr,                        //misma funcion que se uso en la consulta anterior
                        default: "ProxAgenteLibre",
                        output: {
                            "count": { $sum: 1 },
                            "jugador":
                            {
                                $push: {                  //campos que queremos mantener del estadío anterior
                                    "nombre": "$nombre",
                                    "fin_contrato": "$finContrato",
                                    "ValorMercado": "$valorM€"
                                }
                            }
                        }
                    }
                }
            ]
        }
    }
]).pretty()

/*/*{
        "valor" : [
                {
                        "_id" : 0,
                        "count" : 16,
                        "jugador" : [
                                {
                                        "nombre" : "Kike Garcia",
                                        "valorM€" : 2
                                },
                                {
                                        "nombre" : "Bryan Gil",
                                        "valorM€" : 15
                                },
                                {
                                        "nombre" : "Sergio Canales",
                                        "valorM€" : 20
                                },
                                {
                                        "nombre" : "Fernando Niño",
                                        "valorM€" : 3
                                },
                                {
                                        "nombre" : "Youssef En-Nesyri",
                                        "valorM€" : 15
                                },
                                {
                                        "nombre" : "Luis Suarez",
                                        "valorM€" : 15
                                },
                                {
                                        "nombre" : "Sergio Ramos",
                                        "valorM€" : 14
                                },
                                {
                                        "nombre" : "Denis Suarez",
                                        "valorM€" : 10
                                },
                                {
                                        "nombre" : "Cesc Fabregas",
                                        "valorM€" : 6.5
                                },
                                {
                                        "nombre" : "Hamari Traore",
                                        "valorM€" : 8
                                },
                                {
                                        "nombre" : "Sven Botman",
                                        "valorM€" : 23
                                },
                                {
                                        "nombre" : "JC Todibo",
                                        "valorM€" : 12
                                },
                                {
                                        "nombre" : "Juan Mata",
                                        "valorM€" : 7.5
                                },
                                {
                                        "nombre" : "Jamie Vardy",
                                        "valorM€" : 16
                                },
                                {
                                        "nombre" : "Andreas Christensen",
                                        "valorM€" : 24
                                },
                                {
                                        "nombre" : "Gareth Bale",
                                        "valorM€" : 20
                                }
                        ],
                        "edadMedia" : 27.6875
                },
                {
                        "_id" : 25,
                        "count" : 7,
                        "jugador" : [
                                {
                                        "nombre" : "Iñigo Martinez",
                                        "valorM€" : 25
                                },
                                {
                                        "nombre" : "Nabil Fekir",
                                        "valorM€" : 25
                                },
                                {
                                        "nombre" : "Vinicius Jr",
                                        "valorM€" : 40
                                },
                                {
                                        "nombre" : "Lucas Ocampos",
                                        "valorM€" : 45
                                },
                                {
                                        "nombre" : "Yannick Carrasco",
                                        "valorM€" : 30
                                },
                                {
                                        "nombre" : "Florian Thauvin",
                                        "valorM€" : 32
                                },
                                {
                                        "nombre" : "Sergio Aguero",
                                        "valorM€" : 35
                                }
                        ],
                        "edadMedia" : 27
                },
                {
                        "_id" : 50,
                        "count" : 6,
                        "jugador" : [
                                {
                                        "nombre" : "Jules Kounde",
                                        "valorM€" : 50
                                },
                                {
                                        "nombre" : "Toni Kroos",
                                        "valorM€" : 50
                                },
                                {
                                        "nombre" : "Ansu Fati",
                                        "valorM€" : 80
                                },
                                {
                                        "nombre" : "Houssem Aouar",
                                        "valorM€" : 50
                                },
                                {
                                        "nombre" : "Virgil van Dijk",
                                        "valorM€" : 80
                                },
                                {
                                        "nombre" : "Thomas Partey",
                                        "valorM€" : 50
                                }
                        ],
                        "edadMedia" : 24.833333333333332
                },
                {
                        "_id" : 100,
                        "count" : 1,
                        "jugador" : [
                                {
                                        "nombre" : "Kylian Mbappe",
                                        "valorM€" : 180
                                }
                        ],
                        "edadMedia" : 22
                }
        ],
        "añoContrato" : [
                {
                        "_id" : "ProxAgenteLibre",
                        "count" : 7,
                        "jugador" : [
                                {
                                        "nombre" : "Bryan Gil",
                                        "fin_contrato" : ISODate("2021-06-29T22:00:00Z"),
                                        "ValorMercado" : 15
                                },
                                {
                                        "nombre" : "Sergio Ramos",
                                        "fin_contrato" : ISODate("2021-06-29T22:00:00Z"),
                                        "ValorMercado" : 14
                                },
                                {
                                        "nombre" : "Florian Thauvin",
                                        "fin_contrato" : ISODate("2021-06-29T22:00:00Z"),
                                        "ValorMercado" : 32
                                },
                                {
                                        "nombre" : "JC Todibo",
                                        "fin_contrato" : ISODate("2021-06-29T22:00:00Z"),
                                        "ValorMercado" : 12
                                },
                                {
                                        "nombre" : "Sergio Aguero",
                                        "fin_contrato" : ISODate("2021-06-29T22:00:00Z"),
                                        "ValorMercado" : 35
                                },
                                {
                                        "nombre" : "Juan Mata",
                                        "fin_contrato" : ISODate("2021-06-29T22:00:00Z"),
                                        "ValorMercado" : 7.5
                                },
                                {
                                        "nombre" : "Gareth Bale",
                                        "fin_contrato" : ISODate("2021-06-29T22:00:00Z"),
                                        "ValorMercado" : 20
                                }
                        ]
                },
                {
                        "_id" : ISODate("2021-06-30T00:00:00Z"),
                        "count" : 5,
                        "jugador" : [
                                {
                                        "nombre" : "Lucas Ocampos",
                                        "fin_contrato" : ISODate("2022-06-29T22:00:00Z"),
                                        "ValorMercado" : 45
                                },
                                {
                                        "nombre" : "Kylian Mbappe",
                                        "fin_contrato" : ISODate("2022-06-29T22:00:00Z"),
                                        "ValorMercado" : 180
                                },
                                {
                                        "nombre" : "Cesc Fabregas",
                                        "fin_contrato" : ISODate("2022-06-29T22:00:00Z"),
                                        "ValorMercado" : 6.5
                                },
                                {
                                        "nombre" : "Hamari Traore",
                                        "fin_contrato" : ISODate("2022-06-29T22:00:00Z"),
                                        "ValorMercado" : 8
                                },
                                {
                                        "nombre" : "Andreas Christensen",
                                        "fin_contrato" : ISODate("2022-06-29T22:00:00Z"),
                                        "ValorMercado" : 24
                                }
                        ]
                },
                {
                        "_id" : ISODate("2022-06-30T00:00:00Z"),
                        "count" : 9,
                        "jugador" : [
                                {
                                        "nombre" : "Iñigo Martinez",
                                        "fin_contrato" : ISODate("2023-06-29T22:00:00Z"),
                                        "ValorMercado" : 25
                                },
                                {
                                        "nombre" : "Kike Garcia",
                                        "fin_contrato" : ISODate("2023-06-29T22:00:00Z"),
                                        "ValorMercado" : 2
                                },
                                {
                                        "nombre" : "Nabil Fekir",
                                        "fin_contrato" : ISODate("2023-06-29T22:00:00Z"),
                                        "ValorMercado" : 25
                                },
                                {
                                        "nombre" : "Toni Kroos",
                                        "fin_contrato" : ISODate("2023-06-29T22:00:00Z"),
                                        "ValorMercado" : 50
                                },
                                {
                                        "nombre" : "Sergio Canales",
                                        "fin_contrato" : ISODate("2023-06-29T22:00:00Z"),
                                        "ValorMercado" : 20
                                },
                                {
                                        "nombre" : "Denis Suarez",
                                        "fin_contrato" : ISODate("2023-06-29T22:00:00Z"),
                                        "ValorMercado" : 10
                                },
                                {
                                        "nombre" : "Houssem Aouar",
                                        "fin_contrato" : ISODate("2023-06-29T22:00:00Z"),
                                        "ValorMercado" : 50
                                },
                                {
                                        "nombre" : "Virgil van Dijk",
                                        "fin_contrato" : ISODate("2023-06-29T22:00:00Z"),
                                        "ValorMercado" : 80
                                },
                                {
                                        "nombre" : "Jamie Vardy",
                                        "fin_contrato" : ISODate("2023-06-29T22:00:00Z"),
                                        "ValorMercado" : 16
                                }
                        ]
                },
                {
                        "_id" : ISODate("2023-06-30T00:00:00Z"),
                        "count" : 4,
                        "jugador" : [
                                {
                                        "nombre" : "Jules Kounde",
                                        "fin_contrato" : ISODate("2024-06-29T22:00:00Z"),
                                        "ValorMercado" : 50
                                },
                                {
                                        "nombre" : "Fernando Niño",
                                        "fin_contrato" : ISODate("2024-06-29T22:00:00Z"),
                                        "ValorMercado" : 3
                                },
                                {
                                        "nombre" : "Luis Suarez",
                                        "fin_contrato" : ISODate("2024-06-29T22:00:00Z"),
                                        "ValorMercado" : 15
                                },
                                {
                                        "nombre" : "Yannick Carrasco",
                                        "fin_contrato" : ISODate("2024-06-29T22:00:00Z"),
                                        "ValorMercado" : 30
                                }
                        ]
                },
                {
                        "_id" : ISODate("2024-06-30T00:00:00Z"),
                        "count" : 5,
                        "jugador" : [
                                {
                                        "nombre" : "Vinicius Jr",
                                        "fin_contrato" : ISODate("2025-06-29T22:00:00Z"),
                                        "ValorMercado" : 40
                                },
                                {
                                        "nombre" : "Ansu Fati",
                                        "fin_contrato" : ISODate("2025-06-29T22:00:00Z"),
                                        "ValorMercado" : 80
                                },
                                {
                                        "nombre" : "Youssef En-Nesyri",
                                        "fin_contrato" : ISODate("2025-06-29T22:00:00Z"),
                                        "ValorMercado" : 15
                                },
                                {
                                        "nombre" : "Sven Botman",
                                        "fin_contrato" : ISODate("2025-06-29T22:00:00Z"),
                                        "ValorMercado" : 23
                                },
                                {
                                        "nombre" : "Thomas Partey",
                                        "fin_contrato" : ISODate("2025-06-29T22:00:00Z"),
                                        "ValorMercado" : 50
                                }
                        ]
                }
        ]
}*/



// Relacion de patrocinadores y jugadores en busca de posibles bonificaciones generadas por
// tener equipo y jugador el mismo sponsor, dandole mas valía al jugador.


db.patrocinador.aggregate([
    { $match: { "idPatrocinador": { $eq: "001" } } },
    {
        $lookup: {
            "localField": "idPatrocinador",
            "from": "jugador",
            "foreignField": "patrocinador",
            "as": "patrocinio"
        }
    },
    {
        $project: {
            _id: 0,
            idPatrocinador: 1,
            nombre: 1,
            contratoEquipo: 1,
            contratoJugador: 1,
            caractJugador: {
                $map: {                    //operador de Arrays
                    input: {
                        $zip: {             //Union y distribucion de Arrays distintas
                            inputs: ["$patrocinio.nombre",         //campos introducidos para la formacion del nuevo array 
                                "$patrocinio.equipo",
                                "$patrocinio.patrocinador"]
                        }
                    },
                    as: "el",               //"el" es de "elemento", ordenacion de los valores en array de salida
                    in: {
                        nombre: { $arrayElemAt: ["$$el", 0] },
                        equipo: { $arrayElemAt: ["$$el", 1] },
                        patrocinador: { $arrayElemAt: ["$$el", 2] }
                    }
                }
            }
        }
    },
    { $unwind: "$caractJugador" },
    {
        $lookup: {
            "localField": "idPatrocinador",
            "from": "equipo",
            "foreignField": "patrocinador.CodP",
            "as": "sponsorEquipo"
        }
    },
    { $unwind: "$sponsorEquipo" },
    {
        $project: {
            idPatrocinador: 1,
            nombre: 1,
            contratoEquipo: 1,
            contratoJugador: 1,
            caractJugador: 1,
            nombreClub: "$sponsorEquipo.nombreClub",
            patroClub: "$sponsorEquipo.patrocinador.CodP"
        }
    },
    { $match: { $expr: { $eq: ["$caractJugador.equipo", "$nombreClub"] } } }
]).pretty()





//Despues del 1 lookup

/*
/*{ "idPatrocinador" : "001", "contratoEquipo" : [ { "tipoContrato" : "A", "contratoM€" : 10 }, { "tipoContrato" : "B", "contratoM€" : 6 } ], "contratoJugador" : [ { "tipoContrato" : "A", "contratoM€" : 2 }, { "tipoContrato" : "B", "contratoM€" : 1.5 } ], 
"nombre" : [ "Jules Kounde", "Nabil Fekir", "Toni Kroos", "Bryan Gil", "Youssef En-Nesyri", "Lucas Ocampos", 
"Luis Suarez", "Sergio Ramos", "Yannick Carrasco", "Kylian Mbappe", "Cesc Fabregas", "Virgil van Dijk", "Sergio Aguero" ], 
"equipo" : [ "SEV", "BET", "RMD", "EIB", "SEV", "ATM", "ATM", "RMD", "ATM", "PSG", "ASM", "LIV", "CIT" ], 
"patrocinador" : [ [ "001", "A" ], [ "001", "B" ], [ "001", "A" ], [ "001", "B" ], [ "001", "B" ], [ "001", "B" ], [ "001", "A" ],*/




// Despues de realizar el $map y $zip

/*{ "idPatrocinador" : "001", "nombre" : "NIKE", "contratoEquipo" : [ { "tipoContrato" : "A", "contratoM€" : 10 }, { "tipoContrato" : "B", "contratoM€" : 6 } ], 
"contratoJugador" : [ { "tipoContrato" : "A", "contratoM€" : 2 }, { "tipoContrato" : "B", "contratoM€" : 1.5 } ], 
"caracteristicasJugador" : [ { "nombre" : "Jules Kounde", "equipo" : "SEV", "patrocinador" : [ "001", "A" ] }, { "nombre" : "Nabil Fekir", "equipo" : "BET", "patrocinador" : [ "001", "B" ] }, 
{ "nombre" : "Toni Kroos", "equipo" : "RMD", "patrocinador" : [ "001", "A" ] },
{ "nombre" : "Bryan Gil", "equipo" : "EIB", "patrocinador" : [ "001", "B" ] }, { "nombre" : "Youssef En-Nesyri", "equipo" : "SEV", "patrocinador" : [ "001", "B" ] }, { "nombre" : "Lucas Ocampos", "equipo" : "ATM", "patrocinador" : [ "001", "B" ] }, 
{ "nombre" : "Luis Suarez", "equipo" : "ATM", "patrocinador" : [ "001", "A" ] }, { "nombre" : "Sergio Ramos", "equipo" : "RMD", "patrocinador" : [ "001", "A" ] }, { "nombre" : "Yannick Carrasco", "equipo" : "ATM", "patrocinador" : [ "001", "B" ] }, { "nombre" : "Kylian Mbappe", "equipo" : "PSG", "patrocinador" : [ "001", "A" ] }, { "nombre" : "Cesc Fabregas", "equipo" : "ASM", "patrocinador" : [ "001", "B" ] 
}, { "nombre" : "Virgil van Dijk", "equipo" : "LIV", "patrocinador" : [ "001", "B" ] }, { "nombre" : "Sergio Aguero", "equipo" : "CIT", "patrocinador" : [ "001", "A" ] } ] }*/





// al Hacer el unwind de sponsorEquipo (solo se muestra el primer documento generado)


/*{
        "idPatrocinador" : "001",
        "nombre" : "NIKE",
        "contratoEquipo" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 10
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 6
                }
        ],
        "contratoJugador" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 2
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 1.5
                }
        ],
        "caracteristicasJugador" : {
                "nombre" : "Lucas Ocampos",
                "equipo" : "SEV",
                "patrocinador" : [
                        "001",
                        "B"
                ]
        },
        "sponsorEquipo" : [
                {
                        "_id" : ObjectId("6038114c73f7740016dd88e8"),
                        "liga" : "ESP",
                        "nombreClub" : "RSD",
                        "PJ" : 35,
                        "golT" : 50,
                        "valorTM€" : 337.9,
                        "patrocinador" : [
                                "001",
                                "A"
                        ]
                },
                {
                        "_id" : ObjectId("6038114c73f7740016dd88ea"),
                        "liga" : "ESP",
                        "nombreClub" : "BET",
                        "PJ" : 27,
                        "golT" : 46,
                        "valorTM€" : 196.4,
                        "patrocinador" : [
                                "001",
                                "A"
                        ]
                },
                {
                        "_id" : ObjectId("6038114c73f7740016dd88ec"),
                        "liga" : "ESP",
                        "nombreClub" : "BCN",
                        "PJ" : 33,
                        "golT" : 79,
                        "valorTM€" : 807,
                        "europa" : true,
                        "patrocinador" : [
                                "001",
                                "A"
                        ]
                },
                {
                        "_id" : ObjectId("6038114c73f7740016dd88ef"),
                        "liga" : "ESP",
                        "nombreClub" : "RMD",
                        "PJ" : 30,
                        "golT" : 52,
                        "valorTM€" : 742,
                        "europa" : true,
                        "patrocinador" : [
                                "001",
                                "A"
                        ]
                },
                {
                        "_id" : ObjectId("6038114c73f7740016dd88f5"),
                        "liga" : "FRA",
                        "nombreClub" : "LYO",
                        "PJ" : 33,
                        "golT" : 60,
                        "valorTM€" : 323.25,
                        "europa" : true,
                        "patrocinador" : [
                                "001",
                                "A"
                        ]
                },
                {
                        "_id" : ObjectId("6038114c73f7740016dd88f9"),
                        "liga" : "ENG",
                        "nombreClub" : "TOT",
                        "PJ" : 24,
                        "golT" : 58,
                        "valorTM€" : 719.65,
                        "patrocinador" : [
                                "001",
                                "A"
                        ]
                },
                {
                        "_id" : ObjectId("6038114c73f7740016dd88fc"),
                        "liga" : "ENG",
                        "nombreClub" : "LIV",
                        "PJ" : 39,
                        "golT" : 60,
                        "valorTM€" : 1120,
                        "europa" : true,
                        "patrocinador" : [
                                "001",
                                "A"
                        ]
                }
        ]
}*/






//Despues del ultimo match

/*{
        "idPatrocinador" : "001",
        "nombre" : "NIKE",
        "contratoEquipo" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 10
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 6
                }
        ],
        "contratoJugador" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 2
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 1.5
                }
        ],
        "caractJugador" : {
                "nombre" : "Jules Kounde",
                "equipo" : "SEV",
                "patrocinador" : [
                        "001",
                        "A"
                ]
        },
        "nombreClub" : "SEV",
        "patroClub" : [
                "001",
                "007"
        ]
}



{
        "idPatrocinador" : "001",
        "nombre" : "NIKE",
        "contratoEquipo" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 10
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 6
                }
        ],
        "contratoJugador" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 2
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 1.5
                }
        ],
        "caractJugador" : {
                "nombre" : "Nabil Fekir",
                "equipo" : "BET",
                "patrocinador" : [
                        "001",
                        "B"
                ]
        },
        "nombreClub" : "BET",
        "patroClub" : [
                "001"
        ]
}
{
        "idPatrocinador" : "001",
        "nombre" : "NIKE",
        "contratoEquipo" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 10
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 6
                }
        ],
        "contratoJugador" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 2
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 1.5
                }
        ],
        "caractJugador" : {
                "nombre" : "Toni Kroos",
                "equipo" : "RMD",
                "patrocinador" : [
                        "001",
                        "A"
                ]
        },
        "nombreClub" : "RMD",
        "patroClub" : [
                "001"
        ]
}
{
        "idPatrocinador" : "001",
        "nombre" : "NIKE",
        "contratoEquipo" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 10
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 6
                }
        ],
        "contratoJugador" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 2
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 1.5
                }
        ],
        "caractJugador" : {
                "nombre" : "Youssef En-Nesyri",
                "equipo" : "SEV",
                "patrocinador" : [
                        "001",
                        "B"
                ]
        },
        "nombreClub" : "SEV",
        "patroClub" : [
                "001",
                "007"
        ]
}
{
        "idPatrocinador" : "001",
        "nombre" : "NIKE",
        "contratoEquipo" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 10
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 6
                }
        ],
        "contratoJugador" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 2
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 1.5
                }
        ],
        "caractJugador" : {
                "nombre" : "Sergio Ramos",
                "equipo" : "RMD",
                "patrocinador" : [
                        "001",
                        "A"
                ]
        },
        "nombreClub" : "RMD",
        "patroClub" : [
                "001"
        ]
}
{
        "idPatrocinador" : "001",
        "nombre" : "NIKE",
        "contratoEquipo" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 10
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 6
                }
        ],
        "contratoJugador" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 2
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 1.5
                }
        ],
        "caractJugador" : {
                "nombre" : "Virgil van Dijk",
                "equipo" : "LIV",
                "patrocinador" : [
                        "001",
                        "B"
                ]
        },
        "nombreClub" : "LIV",
        "patroClub" : [
                "001"
        ]
}
{
        "idPatrocinador" : "001",
        "nombre" : "NIKE",
        "contratoEquipo" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 10
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 6
                }
        ],
        "contratoJugador" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 2
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 1.5
                }
        ],
        "caractJugador" : {
                "nombre" : "Jules Kounde",
                "equipo" : "SEV",
                "patrocinador" : [
                        "001",
                        "A"
                ]
        },
        "nombreClub" : "SEV",
        "patroClub" : [
                "001",
                "007"
        ]
}
{
        "idPatrocinador" : "001",
        "nombre" : "NIKE",
        "contratoEquipo" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 10
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 6
                }
        ],
        "contratoJugador" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 2
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 1.5
                }
        ],
        "caractJugador" : {
                "nombre" : "Nabil Fekir",
                "equipo" : "BET",
                "patrocinador" : [
                        "001",
                        "B"
                ]
        },
        "nombreClub" : "BET",
        "patroClub" : [
                "001"
        ]
}
{
        "idPatrocinador" : "001",
        "nombre" : "NIKE",
        "contratoEquipo" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 10
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 6
                }
        ],
        "contratoJugador" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 2
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 1.5
                }
        ],
        "caractJugador" : {
                "nombre" : "Toni Kroos",
                "equipo" : "RMD",
                "patrocinador" : [
                        "001",
                        "A"
                ]
        },
        "nombreClub" : "RMD",
        "patroClub" : [
                "001"
        ]
}
{
        "idPatrocinador" : "001",
        "nombre" : "NIKE",
        "contratoEquipo" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 10
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 6
                }
        ],
        "contratoJugador" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 2
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 1.5
                }
        ],
        "caractJugador" : {
                "nombre" : "Youssef En-Nesyri",
                "equipo" : "SEV",
                "patrocinador" : [
                        "001",
                        "B"
                ]
        },
        "nombreClub" : "SEV",
        "patroClub" : [
                "001",
                "007"
        ]
}
{
        "idPatrocinador" : "001",
        "nombre" : "NIKE",
        "contratoEquipo" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 10
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 6
                }
        ],
        "contratoJugador" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 2
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 1.5
                }
        ],
        "caractJugador" : {
                "nombre" : "Sergio Ramos",
                "equipo" : "RMD",
                "patrocinador" : [
                        "001",
                        "A"
                ]
        },
        "nombreClub" : "RMD",
        "patroClub" : [
                "001"
        ]
}
{
        "idPatrocinador" : "001",
        "nombre" : "NIKE",
        "contratoEquipo" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 10
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 6
                }
        ],
        "contratoJugador" : [
                {
                        "tipoContrato" : "A",
                        "contratoM€" : 2
                },
                {
                        "tipoContrato" : "B",
                        "contratoM€" : 1.5
                }
        ],
        "caractJugador" : {
                "nombre" : "Virgil van Dijk",
                "equipo" : "LIV",
                "patrocinador" : [
                        "001",
                        "B"
                ]
        },
        "nombreClub" : "LIV",
        "patroClub" : [
                "001"
        ]
}*/
