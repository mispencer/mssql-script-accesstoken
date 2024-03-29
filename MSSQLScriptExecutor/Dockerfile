#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app

FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG TARGETARCH
WORKDIR /src
COPY ["MSSQLScriptExecutor.csproj", ""]
RUN dotnet restore -a "$TARGETARCH" "./MSSQLScriptExecutor.csproj"
COPY . .
WORKDIR "/src/."
RUN dotnet build -a "$TARGETARCH" "MSSQLScriptExecutor.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish -a "$TARGETARCH" "MSSQLScriptExecutor.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "MSSQLScriptExecutor.dll"]
