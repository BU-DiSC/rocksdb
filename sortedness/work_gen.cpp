#include <fstream>
#include <ctime>
#include <algorithm>
#include <vector>
#include <iostream>
#include <unordered_set>

inline bool ledger_exists();
void generate_one_file(unsigned long long pTOTAL_NUMBERS, unsigned int pdomain, unsigned long long pL, short ppercent_outRange, short plpercentage, int pseed);
unsigned int get_number_domain(unsigned long long position, unsigned long long total, unsigned int domain_);
std::string generate_partitions_stream(unsigned long long TOTAL_NUMBERS, unsigned int domain, unsigned long long L, short percent_outRange, short l_percentage, int seed, std::string folder, std::string type);

inline bool ledger_exists()
{
    std::ifstream f("dataledger.txt");
    return f.good();
}

void generate_one_file(unsigned long long pTOTAL_NUMBERS, unsigned int pdomain, unsigned long long pL, short ppercent_outRange, short plpercentage, int pseed, std::string type)
{
    std::ofstream outfile;

    srand(time(NULL));
    outfile.open("dataledger.txt", std::ios_base::app);

    std::string folder_name = "workload/";
    // std::string folder_name = "exp_workloads/";
    // std::string folder_name = "vary_buffer_workload/";
    //    std::string folder_name = "sorting_workload/";
    outfile << generate_partitions_stream(pTOTAL_NUMBERS, pdomain, pL, ppercent_outRange, plpercentage, pseed, folder_name, type) << std::endl;

    outfile.close();
}

unsigned int get_number_domain(unsigned long long position, unsigned long long total, unsigned int domain_)
{
    return (position * domain_) / total;
}

unsigned int generate_random_in_range(unsigned long long position, unsigned long long Total_Numbers, double l_percentage)
{
    unsigned int l = Total_Numbers * l_percentage;

    if ((int)(position - l) <= 0)
    {
        //return (rand() % position) + (position + l);
        return (rand() % (position + l));
    }
    else if ((int)(position + l) >= Total_Numbers)
    {
        //return (rand() % (position - l)) + position;
        return (rand() % (Total_Numbers - (position - l))) + (position - l);
    }
    else
    {
        //return (rand() % (position - l)) + (position + l);
        return (rand() % ((position + l) - (position - l))) + (position - l);
    }
}

/*
    Function which generates uniform data over some domain, and write it in binary format.
    Each partition of L elements is shuffled, and has some noise (randomness) linked to the
    percent_outRange parameter.
    */
std::string generate_partitions_stream(unsigned long long TOTAL_NUMBERS, unsigned int domain, unsigned long long L, short percent_outRange, short l_percentage, int seed, std::string folder = "./Data", std::string type = "bin")
{
    float p_outOfRange = percent_outRange / 100.0;

    std::srand(seed);

    //unsigned long long array[TOTAL_NUMBERS];
    std::vector<unsigned long long> array(TOTAL_NUMBERS, 0);

    std::string fname = folder;
    fname += "/createdata_";
    fname += std::to_string(TOTAL_NUMBERS);
    fname += "-elems_";
    fname += std::to_string(percent_outRange);
    fname += "-noise_";
    fname += std::to_string(l_percentage);
    fname += "-lpercentage_";
    fname += std::to_string(seed);
    fname += "seed";
    fname += std::to_string(std::time(nullptr));
    std::cout << "Type = " << type << std::endl;
    if (type.compare("txt") == 0)
        fname += ".txt";
    else
        fname += ".dat";

    std::string f1name = folder;
    f1name += "/createdata_";
    f1name += std::to_string(TOTAL_NUMBERS);
    f1name += "-elems_";
    f1name += std::to_string(percent_outRange);
    f1name += "-noise_";
    f1name += std::to_string(l_percentage);
    f1name += "-lpercentage_";
    f1name += std::to_string(seed);
    f1name += "seed";
    f1name += std::to_string(std::time(nullptr));

    std::ofstream myfile1;
    // f1name += ".dat";
    if (type.compare("txt") == 0)
    {
        f1name += ".txt";
        myfile1.open(f1name);
        
    }
        
    else
    {
        f1name += ".dat";
        myfile1.open(f1name, std::ios::binary);
    }
        

    //std::ofstream myfile(fname, std::ios::binary);
    
    // if (type.compare("txt") == 0)
    //     myfile1(f1name);

    unsigned long long noise_limit = TOTAL_NUMBERS * p_outOfRange;
    unsigned long long noise_counter = 0;

    std::unordered_set<unsigned long long> myset;
    unsigned long long w = 0; 
    for (unsigned long long i = 0; i < TOTAL_NUMBERS; i++, w+=L)
    {
        array[i] = w;
    }
    for (unsigned long long i = 0; i < TOTAL_NUMBERS; i++)
    {

        float ran = static_cast<float>(rand()) / static_cast<float>(RAND_MAX);
        //randomize generation
        if (ran < p_outOfRange && noise_counter < noise_limit)
        {
            //generate position of shuffle
            //unsigned long long r = (rand() % TOTAL_NUMBERS);
            unsigned long long r;
            while (true)
            {
                r = generate_random_in_range(i, TOTAL_NUMBERS, l_percentage / 100.0);
                if (myset.find(r) != myset.end())
                {
                    continue;
                }
                else
                {
                    break;
                }
            }
            myset.insert(r);

            unsigned long long temp = array[i];
            array[i] = array[r];
            array[r] = temp;

            noise_counter++;
        }
    }
    // for (unsigned long long i = 0; i < TOTAL_NUMBERS; i++)
    // {
    //     array[i] = i;
    //     // myset.insert(i);

    //     float ran = static_cast<float>(rand()) / static_cast<float>(RAND_MAX);
    //     //randomize generation
    //     if (ran < p_outOfRange && noise_counter < noise_limit)
    //     {
    //         //generate position of shuffle
    //         //unsigned long long r = (rand() % TOTAL_NUMBERS);
    //         unsigned long long r;
    //         while (true)
    //         {
    //             r = generate_random_in_range(i, TOTAL_NUMBERS, l_percentage / 100.0);
    //             if (myset.find(r) != myset.end())
    //             {
    //                 continue;
    //             }
    //             else
    //             {
    //                 break;
    //             }
    //         }
    //         myset.insert(r);

    //         if (array[r] == 0)
    //         {
    //             if (r != 0)
    //             {
    //                 array[r] = r;
    //             }
    //         }

    //         unsigned long long temp = array[i];
    //         array[i] = array[r];
    //         array[r] = temp;

    //         noise_counter++;
    //     }
    // }

    if(type.compare("txt")==0)
    {
for (unsigned long long j = 0; j < TOTAL_NUMBERS; ++j)
    {
        // auto pair = std::make_pair(array[j], array[j]);
        // //myfile.write(reinterpret_cast<char *>(&pair), sizeof(std::pair<unsigned int, unsigned int>));
        // myfile1.write(reinterpret_cast<char *>(&array[j]), sizeof(int));
        myfile1<<array[j]<<",";
    }

    }
    else 
    {
        for (unsigned long long j = 0; j < TOTAL_NUMBERS; ++j)
    {
        auto pair = std::make_pair(array[j], array[j]);
        //myfile.write(reinterpret_cast<char *>(&pair), sizeof(std::pair<unsigned int, unsigned int>));
        myfile1.write(reinterpret_cast<char *>(&array[j]), sizeof(int));
    }

    }
    
    //myfile.close();
    myfile1.close();

    return fname;
}

//arguments to program:
//unsigned long long pTOTAL_NUMBERS, unsigned int pdomain, unsigned long long pL, short ppercent_outRange, int pseed

int main(int argc, char **argv)
{
    if (argc < 7)
    {
        std::cout << "Program requires 6 inputs as parameters. \n Use format: ./execs/workload_generator.exe totalNumbers domain windowSize noisePercentage lPercentageThreshold seedValue type" << std::endl;
        return 0;
    }

    unsigned long long totalNumbers = atoi(argv[1]);
    unsigned int domain = atoi(argv[2]);
    unsigned long long windowSize = atoi(argv[3]);
    short noisePercentage = atoi(argv[4]);
    short lPercentage = atoi(argv[5]);
    int seedValue = atoi(argv[6]);
    std::string type = argv[7];

    generate_one_file(totalNumbers, domain, windowSize, noisePercentage, lPercentage, seedValue, type);
}
