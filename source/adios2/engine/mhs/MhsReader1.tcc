/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * MhsReader.tcc
 *
 *  Created on: Aug 04, 2021
 *      Author: Jason Wang jason.ruonan.wang@gmail.com
 */

#ifndef ADIOS2_ENGINE_MHSREADER_TCC_
#define ADIOS2_ENGINE_MHSREADER_TCC_

#include "MhsReader.h"

namespace adios2
{
namespace core
{
namespace engine
{

template <class T>
inline void MhsReader::GetSyncCommon(Variable<T> &variable, T *data)
{
  GetDeferredCommon(variable, data);
  PerformGets();
}

template <>
inline void MhsReader::GetDeferredCommon(Variable<std::string> &variable,
                                         std::string *data)
{
  if(m_Mode == "file" || m_Mode == "daemon" )
  {
    m_SubEngines[0]->Get(variable, data, Mode::Sync);
  }
  else
  {
  }
}

template <class T>
void MhsReader::GetDeferredCommon(Variable<T> &variable, T *data)
{
  if(m_Mode == "file" || m_Mode == "daemon" )
  {
    for (int i = 0; i < m_Tiers; ++i)
    {
        auto var = m_SubIOs[i]->InquireVariable<T>(variable.m_Name);
        if (!var)
        {
            break;
        }
        var->SetSelection({variable.m_Start, variable.m_Count});
        m_SubEngines[i]->Get(*var, data, Mode::Sync);
        if (m_SiriusCompressor->m_CurrentReadFinished)
        {
            break;
        }
    }
  }
  else
  {
    std::cout<<"\n****** tcc get function \n";
  
    auto stepInquire = m_InquireIO->InquireVariable<int>("InquireStep");
    auto varInquire = m_InquireIO->InquireVariable<char>("InquireVarName");
    auto startInquire = m_InquireIO->InquireVariable<uint64_t>("InquireStart");
    auto countInquire = m_InquireIO->InquireVariable<uint64_t>("InquireCount");

    
    int inquire_step = variable.m_StepsStart; //int inquire_step = CurrentStep();
    
    std::cout<<"\n****** variable.step: "<<variable.m_StepsStart<<", start: "<< variable.m_Start<<", count: "<<variable.m_Count;
    std::cout<<"\n****** variable.start.size: "<< variable.m_Start.size()<<", count.size: "<<variable.m_Count.size()<<std::endl;
   
    std::vector<uint64_t> inquire_start(variable.m_Start.begin(), variable.m_Start.end());
    std::vector<uint64_t> inquire_count(variable.m_Count.begin(),variable.m_Count.end());
    
    m_InquireEngine->BeginStep();
    std::cout<<"\n m_InquireEngine->BeginStep()\n";
    varInquire->SetShape({variable.m_Name.size()});
    startInquire->SetShape({variable.m_Start.size()+1});
    countInquire->SetShape({variable.m_Count.size()+1});

    
    varInquire->SetSelection({{0},{variable.m_Name.size()}});
    startInquire->SetSelection({{0},{variable.m_Start.size()}});
    countInquire->SetSelection({{0},{variable.m_Count.size()}});
    //startInquire->SetSelection({{0},{variable.m_Start.size()*sizeof(size_t)}});
    //countInquire->SetSelection({{0},{variable.m_Count.size()*sizeof(size_t)}});

    m_InquireEngine->Put(*stepInquire, &inquire_step);
    m_InquireEngine->Put(*startInquire, inquire_start.data(), adios2::Mode::Sync);
    m_InquireEngine->Put(*countInquire, inquire_count.data(), adios2::Mode::Sync);
    m_InquireEngine->Put(*varInquire, variable.m_Name.c_str(), adios2::Mode::Sync);
    m_InquireEngine->EndStep();
    
    auto status = m_RemoteEngine->BeginStep();
    std::cout<<"m_RemoteEngine->BeginStep()\n";

   if (status == adios2::StepStatus::OK)
    { 
      std::cout<<"m_RemoteEngine adios2::StepStatus::OK\n";

      auto varRemote = m_RemoteIO->InquireVariable<T>(variable.m_Name);
      varRemote->SetSelection({variable.m_Start, variable.m_Count});
      m_RemoteEngine->Get<T>(*varRemote, data, adios2::Mode::Sync);
      std::cout<<"\nRequest :"<<variable.m_Name.c_str()<<", step"<<variable.m_StepsStart<<", start: "<<variable.m_Start <<", count: "<<variable.m_Count<<std::endl;
      
      /** Write to new directory * */
     
      auto varSmartCache = m_SmartCacheIO->InquireVariable<T>(variable.m_Name.c_str());
      std::string varSmartCacheName(variable.m_Name.c_str());

     if(!varSmartCache){
	int varRemoteDims = varRemote->Shape().size();
	std::cout<<"\n========== name: "<<variable.m_Name<<", start: "<<variable.m_Start<<", count: "<<variable.m_Count <<std::endl;
    	varSmartCache = &m_SmartCacheIO->DefineVariable<T>(varSmartCacheName, varRemote->Shape(),variable.m_Start, variable.m_Count);
         
      }

      m_SmartCacheEngine->BeginStep();

      if(varSmartCache){
          
         m_SmartCacheEngine->Put<T>(*varSmartCache, data, adios2::Mode::Sync);
      	 m_SmartCacheEngine->EndStep();

      } 
      
      m_RemoteEngine->EndStep();
    }
     
  }
}

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif // ADIOS2_ENGINE_MHSREADER_TCC_
