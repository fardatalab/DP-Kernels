#include <stdio.h>
#include <string.h>

#include "ControlPlaneHandler.h"
#include "DPUBackEnd.h"
#include "DPUBackEndStorage.h"
#include "DPKEmbedShim.h"

//
// Handler for a control plane request
//
// jason: FIND_FIRST_FILE is handled synchronously here to match the
// control-plane completion path used by other requests.
//
void ControlPlaneHandler(
    void *ReqContext
) {
    //
    // Upon completion, set Context->Response->Result as DDS_ERROR_CODE_SUCCESS
    //
    //
    ControlPlaneRequestContext *Context = (ControlPlaneRequestContext *)ReqContext;
    printf("ControlPlaneHandler: %d\n", Context->RequestId);
    switch (Context->RequestId) {
        case CTRL_MSG_F2B_REQ_CREATE_DIR: {
            CtrlMsgF2BReqCreateDirectory *Req = (CtrlMsgF2BReqCreateDirectory *)Context->Request;
            CtrlMsgB2FAckCreateDirectory *Resp = (CtrlMsgB2FAckCreateDirectory *)Context->Response;

            //
            // We should free handler ctx in the last callback for every case
            //
            //
            ControlPlaneHandlerCtx *HandlerCtx = malloc(sizeof(*HandlerCtx));
            HandlerCtx->Result = &Resp->Result;
            CreateDirectory(
                Req->PathName,
                Req->DirId,
                Req->ParentDirId,
                Sto,
                Context->SPDKContext,
                HandlerCtx
            );
        }
            break;
        case CTRL_MSG_F2B_REQ_REMOVE_DIR: {
            CtrlMsgF2BReqRemoveDirectory *Req = (CtrlMsgF2BReqRemoveDirectory *)Context->Request;
            CtrlMsgB2FAckRemoveDirectory *Resp = (CtrlMsgB2FAckRemoveDirectory *)Context->Response;
            ControlPlaneHandlerCtx *HandlerCtx = malloc(sizeof(*HandlerCtx));
            HandlerCtx->Result = &Resp->Result;
            RemoveDirectory(
                Req->DirId,
                Sto,
                Context->SPDKContext,
                HandlerCtx
            );

        }
            break;
            case CTRL_MSG_F2B_REQ_FIND_FIRST_FILE:
            {
                CtrlMsgF2BReqFindFirstFile *Req = (CtrlMsgF2BReqFindFirstFile *)Context->Request;
                CtrlMsgB2FAckFindFirstFile *Resp = (CtrlMsgB2FAckFindFirstFile *)Context->Response;

                printf("FIND_FIRST_FILE for %s\n", Req->FileName);
                // jason: DON'T TOUCH Resp->Result, leave it as is (IO PENDING) until completion
                Resp->FileId = DDS_FILE_INVALID;
                ////Resp->Result = DDS_ERROR_CODE_FILE_NOT_FOUND;
                if (!Req->FileName || !Sto)
                {
                    break;
                }

                for (FileIdT i = 0; i != DDS_MAX_FILES; i++)
                {
                    struct DPUFile *file = Sto->AllFiles[i];
                    if (!file)
                    {
                        continue;
                    }
                    const char *curr_name = GetName(file);
                    printf("Checking FileId %d: %s\n", i, curr_name);
                    if (!curr_name)
                    {
                        continue;
                    }
                    if (strcmp(curr_name, Req->FileName) == 0)
                    {
                        Resp->FileId = i;
                        Resp->Result = DDS_ERROR_CODE_SUCCESS;
                        printf("FIND_FIRST_FILE found FileId %d for %s\n", i, Req->FileName);
                        break;
                    }
                }
            }
            break;
            case CTRL_MSG_F2B_REQ_CREATE_FILE:
            {
                CtrlMsgF2BReqCreateFile *Req = (CtrlMsgF2BReqCreateFile *)Context->Request;
                CtrlMsgB2FAckCreateFile *Resp = (CtrlMsgB2FAckCreateFile *)Context->Response;
                ControlPlaneHandlerCtx *HandlerCtx = malloc(sizeof(*HandlerCtx));
                HandlerCtx->Result = &Resp->Result;
                CreateFile(Req->FileName, Req->FileAttributes, Req->FileId, Req->DirId, Sto, Context->SPDKContext,
                           HandlerCtx);
            }
            break;
        case CTRL_MSG_F2B_REQ_DELETE_FILE: {
            CtrlMsgF2BReqDeleteFile *Req = (CtrlMsgF2BReqDeleteFile *)Context->Request;
            CtrlMsgB2FAckDeleteFile *Resp = (CtrlMsgB2FAckDeleteFile *)Context->Response;
            ControlPlaneHandlerCtx *HandlerCtx = malloc(sizeof(*HandlerCtx));
            HandlerCtx->Result = &Resp->Result;
            DeleteFileOnSto(
                Req->FileId,
                Req->DirId,
                Sto,
                Context->SPDKContext,
                HandlerCtx
            );
        }
            break;
        case CTRL_MSG_F2B_REQ_CHANGE_FILE_SIZE: {
            CtrlMsgF2BReqChangeFileSize *Req = (CtrlMsgF2BReqChangeFileSize *)Context->Request;
            CtrlMsgB2FAckChangeFileSize *Resp = (CtrlMsgB2FAckChangeFileSize *)Context->Response;
            // Original: ChangeFileSize(Req->FileId, Req->NewSize, Sto, Resp);
            // Updated: use control-plane variant that persists metadata.
            ControlPlaneHandlerCtx *HandlerCtx = malloc(sizeof(*HandlerCtx));
            HandlerCtx->Result = &Resp->Result;
            ChangeFileSizeControlPlane(
                Req->FileId,
                Req->NewSize,
                Sto,
                Context->SPDKContext,
                HandlerCtx
            );
        }
            break;
        case CTRL_MSG_F2B_REQ_GET_FILE_SIZE: {
            CtrlMsgF2BReqGetFileSize *Req = (CtrlMsgF2BReqGetFileSize *)Context->Request;
            CtrlMsgB2FAckGetFileSize *Resp = (CtrlMsgB2FAckGetFileSize *)Context->Response;
            GetFileSize(
                Req->FileId,
                &Resp->FileSize,
                Sto,
                Resp
            );
        }
            break;
        case CTRL_MSG_F2B_REQ_GET_FILE_INFO: {
            CtrlMsgF2BReqGetFileInfo *Req = (CtrlMsgF2BReqGetFileInfo *)Context->Request;
            CtrlMsgB2FAckGetFileInfo *Resp = (CtrlMsgB2FAckGetFileInfo *)Context->Response;
            GetFileInformationById(
                Req->FileId,
                &Resp->FileInfo,
                Sto,
                Resp
            );
        }
            break;
        case CTRL_MSG_F2B_REQ_GET_FILE_ATTR: {
            CtrlMsgF2BReqGetFileAttr *Req = (CtrlMsgF2BReqGetFileAttr *)Context->Request;
            CtrlMsgB2FAckGetFileAttr *Resp = (CtrlMsgB2FAckGetFileAttr *)Context->Response;
            GetFileAttributes(
                Req->FileId,
                &Resp->FileAttr,
                Sto,
                Resp
            );
        }
            break;
        case CTRL_MSG_F2B_REQ_GET_FREE_SPACE: {
            CtrlMsgB2FAckGetFreeSpace *Resp = (CtrlMsgB2FAckGetFreeSpace *)Context->Response;
            GetStorageFreeSpace(
                &Resp->FreeSpace,
                Sto,
                Resp
            );
        }
            break;
        case CTRL_MSG_F2B_REQ_GET_IO_ALIGN: {
            CtrlMsgB2FAckGetIoAlign *Resp = (CtrlMsgB2FAckGetIoAlign *)Context->Response;
            // jason: Return cached bdev alignment constraints to the host.
            SPDKContextT *spdkContext = (SPDKContextT *)Context->SPDKContext;
            Resp->BlockSize = spdkContext ? (FileIOSizeT)spdkContext->block_size : 1;
            Resp->BufAlign = spdkContext ? (FileIOSizeT)spdkContext->buf_align : 1;
            Resp->Result = DDS_ERROR_CODE_SUCCESS;
        }
            break;
        case CTRL_MSG_F2B_REQ_SET_READ2_AES_KEY: {
            CtrlMsgF2BReqSetRead2AesKey *Req = (CtrlMsgF2BReqSetRead2AesKey *)Context->Request;
            CtrlMsgB2FAckSetRead2AesKey *Resp = (CtrlMsgB2FAckSetRead2AesKey *)Context->Response;
            if (Req->KeySizeBytes != 16 && Req->KeySizeBytes != 32)
            {
                Resp->Result = DDS_ERROR_CODE_INVALID_PARAM;
                break;
            }
            if (dpk_embed_runtime_is_ready() == 0)
            {
                Resp->Result = DDS_ERROR_CODE_FAILED_CONNECTION;
                break;
            }
            int keyRet = dpk_embed_set_global_aes_key((const void *)Req->KeyBytes, Req->KeySizeBytes);
            Resp->Result = (keyRet == 0) ? DDS_ERROR_CODE_SUCCESS : DDS_ERROR_CODE_IO_FAILURE;
        }
            break;
        case CTRL_MSG_F2B_REQ_MOVE_FILE: {
            CtrlMsgF2BReqMoveFile *Req = (CtrlMsgF2BReqMoveFile *)Context->Request;
            CtrlMsgB2FAckMoveFile *Resp = (CtrlMsgB2FAckMoveFile *)Context->Response;
            ControlPlaneHandlerCtx *HandlerCtx = malloc(sizeof(*HandlerCtx));
            HandlerCtx->Result = &Resp->Result;
            MoveFile(
                Req->FileId,
                Req->OldDirId,
                Req->NewDirId,
                Req->NewFileName,
                Sto,
                Context->SPDKContext,
                HandlerCtx
            );
        }
            break;
        default: {
            printf("UNKNOWN CONTROL PLANE REQUEST: %d\n", Context->RequestId);
        }
    }
}
