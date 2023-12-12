# MARVEL NH3
The purpose of this repository is to track changes throughout the application of the MARVEL procedure (Measured Active Ro-Vibrational Energy Levels) (Furtenbacher et al. 2007) to transitions of the main ammonia isotopologue. The present work is intended to be an update of the previous MARVEL energy levels obtained by (Furtenbacher et al. 2020). Note that the states file is too large to be included in this repository so one may download it from the ExoMol website: https://exomol.com/data/molecules/NH3/14N-1H3/CoYuTe/.

Terminal command to run the MARVEL 4.1 executable:
./MARVEL4.1.x -s segments.txt -t Marvel-14NH3-Main.txt

Alternatively, the latest online version (4.1) of MARVEL is available at: https://respecth.elte.hu/MARVEL4/index.html

# Project Structure
Within the main directory we keep the segments.txt file, needed by MARVEL to specify the units used by transitions of a given segment tag. The file Marvel-14NH3-2020.txt are the transitions from the 2020 MARVEL study. The file Marvel-14NH3-Main.txt is the current updated MARVEL set.

This project also includes a number of directories. Most are divided to be staging areas for extracted MARVEL data, within which the new transitions are converted to the MARVEL 2020 format. These directeries simply follow the naming conventions for the adopted segments during MARVEL studies. 

The directory CombinationDifferencesTests includes a set of MARVEL 2020 energy levels called 14NH3-MarvelEnergies-2020.txt. These are used by the script CombinationDifferences.py, which reads the previous Marvel-14NH3-2020.txt file and appends the new transitions from their respective directories, to apply combination differences tests using the previous MARVEL energy levels as lower states. The script also contains a Python list of source tags called `transitionsToRemove`, which are transitions that we have manually found to be inconsistent through the combination differences procedure or other means of validation. These transitions are invalidated i.e. a minus sign is put in front of the transition frequency. The script concludes by printing the resulting MARVEl transition set in order of ascending transition frequency into the Marvel-14NH3-Main.txt file (it is overwritten upon each run). 

## References
 Furtenbacher, T; Csaszar, AG; Tennyson, J; (2007) MARVEL: measured active rotational-vibrational energy levels. J MOL SPECTROSC , 245 (2) 115 - 125  https://doi.org/10.1016/j.jms.2007.07.005

 Furtenbacher, T; Coles, PA; Tennyson, J; et al; (2020) Empirical rovibrational energy levels of ammonia up to 7500 cm. JQSRT  https://doi.org/10.1016/j.jqsrt.2020.107027