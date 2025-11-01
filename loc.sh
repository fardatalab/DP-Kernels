TotalH=`( find ../ -name '*.h' -print0 | xargs -0 cat ) | wc -l`
TotalC=`( find ../ -name '*.c' -print0 | xargs -0 cat ) | wc -l`
TotalCPP=`( find ../ -name '*.cpp' -print0 | xargs -0 cat ) | wc -l`
TotalSH=`( find ../ -name '*.sh' -print0 | xargs -0 cat ) | wc -l`
TotalLoC=$(( $TotalH + $TotalC + $TotalCPP + $TotalSH ))
echo "Repo LoC = $TotalLoC"

DDSH=`( find ../ -name '*.h' -not -path "../Apps*" -not -path "*out/*" -print0 | xargs -0 cat ) | wc -l`
DDSC=`( find ../ -name '*.c' -not -path "../Apps*" -not -path "*out/*" -print0 | xargs -0 cat ) | wc -l`
DDSCPP=`( find ../ -name '*.cpp' -not -path -not -path "../Apps*" -not -path "*out/*" -print0 | xargs -0 cat ) | wc -l`
DDSHPP=`( find ../ -name '*.hpp' -not -path "../Apps*" -not -path "*out/*" -print0 | xargs -0 cat ) | wc -l`

DDSSH=`( find ../ -name '*.sh' -not -path "*/TLDK*" -not -path "../Apps*" -not -path "../NDSPI*" -not -path "../ThirdParty*" -print0 | xargs -0 cat ) | wc -l`
DDSLoC=$(( $DDSH + $DDSC + $DDSCPP + $DDSSH + $DDSHPP ))
echo "DDS LoC = $DDSLoC"