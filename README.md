##Integrantes
#Vanessa Malverde
#Sebastian Astudillo

##Iniciar Contenedores:

```sh
    docker-compose up --build # Se recomienda quitar --build si no se desea rebuilder.
```

##Funciones:

Se utilizó postman para las pruebas enviando un Body del tipo JSON a través de POST 

#localhost:3000/RegistroMiembro
```json
{
    "nombre":"vanessa", 
    "apellido":"malverde",
    "rut":"1-0",
    "correo":"mail@mail.cl",
    "patente":"22",
    "premium": false
}
```

#localhost:3000/RegistroVenta
```json
{   
    "patente":"22",
    "cliente":"vanessa", 
    "cantidad_sopaipillas":"10",
    "stock_restante":"50",
    "coordenadas":"9,1,1" 
}
```

#localhost:3000/RegistroAviso
```json
{   
    "patente":"22",
    "coordenadas":"0,0,7" 
}
```
