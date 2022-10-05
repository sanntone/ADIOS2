/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * MhsReader.cpp
 *
 *  Created on: Aug 04, 2021
 *      Author: Jason Wang jason.ruonan.wang@gmail.com
 */

#include "MhsReader.tcc"
#include "adios2/helper/adiosFunctions.h"
#include <fstream>
#include <sys/stat.h>

namespace adios2
{
namespace core
{
namespace engine
{

MhsReader::MhsReader(IO &io, const std::string &name, const Mode mode,
                     helper::Comm comm)
: Engine("MhsReader", io, name, mode, std::move(comm))
{

  if(m_Mode == "file" || m_Mode == "daemon" )
  {
    helper::GetParameter(io.m_Parameters, "Mode", m_Mode);
    helper::GetParameter(io.m_Parameters, "Tiers", m_Tiers);
    Params params = {{"Tiers", std::to_string(m_Tiers)}};
    m_SiriusCompressor = std::make_shared<compress::CompressSirius>(params);
    m_IO.SetEngine("");
    m_SubIOs.emplace_back(&io);
    if(m_Mode == "file")
    {
      m_SubEngines.emplace_back(&io.Open(m_Name + ".tier0", mode));
    }
    else{
      m_SubEngines.emplace_back(&io.Open(m_Name + ".tier0", adios2::Mode::ReadRandomAccess ));
    }

    for (int i = 1; i < m_Tiers; ++i)
    {
      m_SubIOs.emplace_back(
          &io.m_ADIOS.DeclareIO("SubIO" + std::to_string(i)));
      m_SubEngines.emplace_back(&m_SubIOs.back()->Open(
            m_Name + ".tier" + std::to_string(i), mode));
    }
  }

  if(m_Mode== "daemon")
  {
    std::string send_dir = m_Name+".tier0";
    std::cout<<"******** Send Metadata \n";
    std::string md_0 = send_dir + "/md.0";
    std::string md_idx = send_dir + "/md.idx";

    std::ifstream is{md_0, std::ios::in | std::ios::binary};
    char value;
    std::vector<char> metaVec1;
    while (is.read(reinterpret_cast<char*>(&value), sizeof(value)))
        metaVec1.push_back(value);
    const std::size_t metaSize1 = metaVec1.size();
    std::cout<<"\n******** *metaSize1: "<<metaSize1;
    is.close();


    std::ifstream is2{md_idx, std::ios::in | std::ios::binary};
    std::vector<char> metaVec2;
    while (is2.read(reinterpret_cast<char*>(&value), sizeof(value)))
        metaVec2.push_back(value);
    const std::size_t metaSize2 = metaVec2.size();
    std::cout<<"\n******** *metaSize2: "<<metaSize2;
    is.close();

    IO *metaIO = &m_IO.m_ADIOS.DeclareIO("metaIO");
    metaIO->SetEngine("DataMan");
    metaIO->SetParameters({{"IPAddress", "10.0.0.31"}, {"Port", "9030"}});
    Engine *metaEngine = &metaIO->Open("metaStream", adios2::Mode::Write);

    auto varMeta1 = metaIO->DefineVariable<char>("varMeta1", {metaSize1}, {0}, {metaSize1});
    auto varMeta2 = metaIO->DefineVariable<char>("varMeta2", {metaSize2}, {0}, {metaSize2});

    metaEngine->BeginStep();
    metaEngine->Put(varMeta1, metaVec1.data(), adios2::Mode::Sync);
    metaEngine->Put(varMeta2, metaVec2.data(), adios2::Mode::Sync);
    metaEngine->EndStep();
    metaEngine->Close();
    std::cout<<"\n********* send meta done *********"<<std::endl;

    m_RemoteIO = &m_IO.m_ADIOS.DeclareIO("RemoteWriteIO");
    m_InquireIO = &m_IO.m_ADIOS.DeclareIO("RemoteInquireIO");

    m_RemoteIO->SetEngine("DataMan");
    m_InquireIO->SetEngine("DataMan");

    //m_RemoteIO->SetParameters({{"IPAddress", "127.0.0.1"}, {"Port", "12300"}});
    //m_InquireIO->SetParameters({{"IPAddress", "127.0.0.1"}, {"Port", "12310"}});
    m_RemoteIO->SetParameters({{"IPAddress", "10.0.0.31"}, {"Port", "9010"}});
    m_InquireIO->SetParameters({{"IPAddress", "10.0.0.31"}, {"Port", "9020"}});

    m_RemoteEngine = &m_RemoteIO->Open("stream", adios2::Mode::Write);
   
    m_InquireEngine = &m_InquireIO->Open("stream", adios2::Mode::ReadRandomAccess);
    std::cout<<"********* InquireEngine Start **********"<<std::endl;

    /* after sendMeta */
   while (true)
    {
        adios2::StepStatus status = m_InquireEngine->BeginStep();
        if (status == adios2::StepStatus::OK)
        {
            int inquire_step;
            auto varStep = m_InquireIO->InquireVariable<int>("InquireStep");
            auto varName= m_InquireIO->InquireVariable<char>("InquireVarName");
      
            auto varStart = m_InquireIO->InquireVariable<uint64_t>("InquireStart");
            auto varCount = m_InquireIO->InquireVariable<uint64_t>("InquireCount");

            std::vector<char> inquire_var(varName->Shape()[0]);
            std::vector<uint64_t> inquire_start(varStart->Shape()[0]-1);
            std::vector<uint64_t> inquire_count(varCount->Shape()[0]-1);

            m_InquireEngine->Get(*varStep, &inquire_step);
            m_InquireEngine->Get(*varName, inquire_var.data(), adios2::Mode::Sync);
            m_InquireEngine->Get(*varStart, inquire_start.data(), adios2::Mode::Sync);
            m_InquireEngine->Get(*varCount, inquire_count.data(), adios2::Mode::Sync);
            
            std::string received_varName(inquire_var.begin(), inquire_var.end());
            std::cout<<"\n******inquireName: "<<received_varName<<", step: "<<inquire_step<<", start: ";
            for(auto a: inquire_start)
                std::cout<<a<<" ";

            std::cout<<", count: ";
            for(auto a: inquire_count)
                std::cout<<a<<" ";
            
            /*const auto &vars = io.GetAvailableVariables();
            std::cout << " \n================ " << std::endl;
            for(const auto &v: vars)
            {
              std::cout << "Variable : " << v.first << std::endl;
              for(const auto &p : v.second)
              {
                std::cout << "    " << p.first << " : " << p.second << std::endl;
              }
            }*/

            auto varFloats = io.InquireVariable<float>(received_varName);
            if(varFloats == nullptr)
            {
              std::cout << " ==========  no such variable : " <<  received_varName << std::endl;
            }
            if(varFloats) {//varFloats
                auto varFloatsRemote = io.InquireVariable<float>(received_varName);
                if(!varFloatsRemote){
                    std::cout << " \n==========  no such variable : " <<received_varName<< std::endl;
                    int varFloatsDims =varFloats->Shape().size();
                    std::vector<size_t> varStart;
                    varStart.resize(varFloatsDims);
                    varFloatsRemote = &m_RemoteIO->DefineVariable<float>(received_varName, varFloats->Shape(), varStart, varFloats->Shape());
                }
//              varFloatsRemote.AddOperation("zfp", {{"accuracy", "0.01"}});
              auto shape = varFloats->Shape();
              std::vector<float> myFloats(std::accumulate(shape.begin(), shape.end(), 1, std::multiplies<size_t>()));

              varFloats->SetStepSelection({inquire_step,1});
             /*
              int varDims =varFloats->Shape().size();
              inquire_start.resize(varDims);
              inquire_count.resize(varDims);
              std::vector<size_t> new_start(inquire_start.begin(), inquire_start.end());
              std::vector<size_t> new_count(inquire_count.begin(), inquire_count.end());
              
              varFloats->SetSelection({new_start, new_count});
              */
              std::vector<size_t> new_start(inquire_start.begin(), inquire_start.end());
              std::vector<size_t> new_count(inquire_count.begin(), inquire_count.end());
                varFloats->SetSelection({new_start, new_count});

              m_SubEngines[0]->Get(*varFloats, myFloats.data(), adios2::Mode::Sync);

              m_RemoteEngine->BeginStep();

              m_RemoteEngine->Put(*varFloatsRemote, myFloats.data(), adios2::Mode::Sync);
              //m_RemoteEngine->Put(*varCurrentStep, &inquire_step);

              m_RemoteEngine->EndStep();

              std::cout <<"\n******* Step "<<inquire_step<<"  's data to remote, dataType: " <<varFloats->m_Type<< "  *******\n";
            }

            m_InquireEngine->EndStep();
        } else if (status == adios2::StepStatus::EndOfStream) {
            break;
        } else if (status == adios2::StepStatus::NotReady) {
            continue;
        }
    }
    
    m_RemoteEngine->Close();

  }
  else if(m_Mode == "inquirer")
  {
    std::string meta_dir = m_Name;
    IO* metaIO = &m_IO.m_ADIOS.DeclareIO("metaIO");
    metaIO->SetEngine("DataMan");
    //metaIO->SetParameters({{"IPAddress", "127.0.0.1"}, {"Port", "12320"}});
    metaIO->SetParameters({{"IPAddress", "10.0.0.31"}, {"Port", "9030"}});
    Engine* metaReader = &metaIO->Open("metaStream", adios2::Mode::Read);

    if (mkdir(meta_dir.c_str(), 0777) == -1){
        std::cerr << "Error :  " << strerror(errno) << std::endl;
    }else{
        std::cout << "\nDirectory created";
    }

    std::string md_0 = meta_dir + "/md.0";
    std::string md_idx = meta_dir + "/md.idx";

    adios2::StepStatus status = metaReader->BeginStep(adios2::StepMode::Read);
    int receiveCnt=0;
    while(true){
        if(status == adios2::StepStatus::OK ){
            auto metaInquire1 = metaIO->InquireVariable<char>("varMeta1");
            auto metaInquire2 = metaIO->InquireVariable<char>("varMeta2");

            if (metaInquire1 && receiveCnt<2){
                receiveCnt += 1;
                std::size_t metaSize1= metaInquire1->SelectionSize();
                std::cout << "\n********** md.id0: "<<metaSize1<<"  **********\n";
                std::vector<char> metaVec1(metaSize1);
                metaInquire1->SetSelection({{0}, {metaSize1}});
                metaReader->Get(*metaInquire1, metaVec1.data(), adios2::Mode::Sync);

                std::ofstream os{md_0.c_str(), std::ios::out | std::ios::binary};
                for (const auto& value : metaVec1)
                    os.write(reinterpret_cast<const char*>(&value), sizeof(value));
                os.close();
            }
            if(metaInquire2 && receiveCnt<2){
                receiveCnt += 1;
                std::size_t metaSize2= metaInquire2->SelectionSize();
                std::cout << "\n********** md.idx: "<<metaSize2<<"  **********\n";
                std::vector<char> metaVec2(metaSize2);
                metaInquire2->SetSelection({{0}, {metaSize2}});
                metaReader->Get(*metaInquire2, metaVec2.data(), adios2::Mode::Sync);
                std::ofstream os{md_idx.c_str(), std::ios::out | std::ios::binary};
                for (const auto& value : metaVec2)
                    os.write(reinterpret_cast<const char*>(&value), sizeof(value));
                os.close();
            }

            metaReader->EndStep();
            if(receiveCnt == 2){
                break;
            }
        }else if (status == adios2::StepStatus::EndOfStream)
        {
            std::cout << "********** Sreaming end **********" << std::endl;
            break;
        }
        else if (status == adios2::StepStatus::NotReady)
        {
            std::cerr << "********** Error : NotReady ********** " << std::endl;
            continue;
        }
    }

   
    IO *metaWriteIO = &m_IO.m_ADIOS.DeclareIO("metaWriteIO");
    Engine *engine = &metaWriteIO->Open(m_Name, adios2::Mode::Read);
    const auto &vars = metaWriteIO->GetAvailableVariables();
    std::cout << "\n*********** All variables as follows save to metaDir: \n";
    for (const auto &variablePair : vars)
    {
        std::string variableName;
        std::string variableType;
        variableName =variablePair.first;
        std::cout << "MyName: " << variablePair.first<< ", my infos: \n";
        for (const auto &parameter : variablePair.second)
        {
            std::cout << parameter.first << ": " << parameter.second
                      << "\n";
        }
        std::cout << "*******************\n";
    }

    metaReader->Close();
    std::cout<<"********* receive meta done *********"<<std::endl;

    m_RemoteIO = &m_IO.m_ADIOS.DeclareIO("Remote");
    m_InquireIO = &m_IO.m_ADIOS.DeclareIO("Inquire");
    m_SmartCacheIO = &m_IO.m_ADIOS.DeclareIO("SmartCache"); // write final full data to disk
   

    m_RemoteIO->SetEngine("DataMan");
    m_InquireIO->SetEngine("DataMan");
    //m_SmartCacheIO->SetEngine("BP4");

    m_RemoteIO->SetParameters({{"IPAddress", "10.0.0.31"}, {"Port", "9010"}});
    m_InquireIO->SetParameters({{"IPAddress", "10.0.0.31"}, {"Port", "9020"}});

    m_RemoteEngine = &m_RemoteIO->Open("Remote", adios2::Mode::Read);
    m_InquireEngine = &m_InquireIO->Open("Inquire", adios2::Mode::Write);
    m_SmartCacheEngine = &m_SmartCacheIO->Open(m_Name+"_cache", adios2::Mode::Write);
   
    
    auto stepInquire = m_InquireIO->DefineVariable<int>("InquireStep", {1},{0},{1});
    auto varInquire = m_InquireIO->DefineVariable<char>("InquireVarName", {8},{0},{8});
    auto startInquire = m_InquireIO->DefineVariable<uint64_t>("InquireStart", {8},{0},{8});
    auto countInquire = m_InquireIO->DefineVariable<uint64_t>("InquireCount", {8},{0},{8});
  }
  

  m_IsOpen = true;
}

MhsReader::~MhsReader()
{
  if(m_Mode == "file" || m_Mode == "daemon" )
  {
    for (int i = 1; i < m_Tiers; ++i)
    {
      m_IO.m_ADIOS.RemoveIO("SubIO" + std::to_string(i));
    }
    if (m_IsOpen)
    {
      DestructorClose(m_FailVerbose);
    }
  }
  else
  {
  }
  m_IsOpen = false;
}

StepStatus MhsReader::BeginStep(const StepMode mode, const float timeoutSeconds)
{
  if(m_Mode == "file" || m_Mode == "daemon" )
  {
    bool endOfStream = false;
    for (auto &e : m_SubEngines)
    {
      auto status = e->BeginStep(mode, timeoutSeconds);
      if (status == StepStatus::EndOfStream)
      {
        endOfStream = true;
      }
    }
    if (endOfStream)
    {
      return StepStatus::EndOfStream;
    }
  }
  else
  {
  }
  return StepStatus::OK;
}

size_t MhsReader::CurrentStep() const { return m_SubEngines[0]->CurrentStep(); }

void MhsReader::PerformGets()
{
  if(m_Mode == "file" || m_Mode == "daemon" )
  {
    for (auto &e : m_SubEngines)
    {
        e->PerformGets();
    }
  }
  else
  {
  }
}

void MhsReader::EndStep()
{
  if(m_Mode == "file" || m_Mode == "daemon" )
  {
    for (auto &e : m_SubEngines)
    {
        e->EndStep();
    }
  }
  else
  {
  }
}

// PRIVATE

#define declare_type(T)                                                        \
    void MhsReader::DoGetSync(Variable<T> &variable, T *data)                  \
    {                                                                          \
        GetSyncCommon(variable, data);                                         \
    }                                                                          \
    void MhsReader::DoGetDeferred(Variable<T> &variable, T *data)              \
    {                                                                          \
        GetDeferredCommon(variable, data);                                     \
    }
ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

void MhsReader::DoClose(const int transportIndex)
{
  if(m_Mode == "file" || m_Mode == "daemon" )
  {
    for (auto &e : m_SubEngines)
    {
        e->Close();
    }
  }
  else
  {
      m_RemoteEngine->Close();
      m_InquireEngine->Close();
      m_SmartCacheEngine->Close();
  }
}

} // end namespace engine
} // end namespace core
} // end namespace adios2
